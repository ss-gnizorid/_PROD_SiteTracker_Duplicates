import os
import io
import imagehash
import numpy as np
import pandas as pd
from typing import List, Tuple, Literal
from PIL import Image, ImageEnhance, ImageFilter

class ImagePermutation:
    """
    ImagePermutation extracts the hash-maps and bytes from a base image, and various transformed versions of that image, to detect if it already exists in an image database.
    This class can perform these operations on individual, or groups of images.

    Attributes:
        resize (List[int]): The desired width and height for resizing.
        mode (Literal['basic', 'advanced']): The processing mode. Defaults to 'basic'.
    """

    def __init__(self, resize: Tuple[int, int] = (256,256), mode: Literal['basic', 'advanced'] = 'basic'):
        """
        Initializes an ImagePermutation instance.

        Args:
            resize (List[int]): The desired [width, height] for resizing.
            mode (Literal['basic', 'advanced'], optional): Processing mode. 
                Defaults to 'basic'. Advanced mode generates more permutations that are less common.
        """
        self.resize = resize
        self._mode = None  # Initialize internal mode storage
        self.mode = mode  # Use the setter for validation.

    @property
    def mode(self) -> str:
        """Getter for mode."""
        return self._mode

    @mode.setter
    def mode(self, value: str):
        """Setter for mode validation."""
        if value not in ('basic', 'advanced'):
            raise ValueError(f'Invalid mode [{value}]. Expected basic or advanced')
        self._mode = value

    def image_to_bytes(self, image) -> bytes:
        with io.BytesIO() as output:
            image.save(output, format='PNG')
            return output.getvalue()

    def generate_permutations(self, image_path) -> dict:
        """
        Generates dictionary of the transformed images
        
        Args:
            image_path: The input image path

        Returns:
            permutations: dictionary of image permuations
        """
        def rotated_image(base_img):
            rotated_image_dict = {}
            for angle in [-5, 5]:
                rotated_image_dict[f"rotated_{angle}_degrees"] = base_img.rotate(
                angle, resample=Image.BICUBIC, fillcolor=128
            )
            return rotated_image_dict

        def zoomed_image(base_img, zoom_amount: float):
            zoomed_image_dict = {}
            w, h = base_img.size
            crop_w, crop_h = int(w * (1-zoom_amount)), int(h * (1 - zoom_amount))
            left = (w - crop_w) // 2
            upper = (h - crop_h) // 2
            zoomed = base_img.crop((left, upper, left + crop_w, upper + crop_h))
            zoomed_image_dict[f'zoomed_{zoom_amount * 100}_percent'] = zoomed.resize(self.resize, Image.LANCZOS)
            return zoomed_image_dict
        
        def brightness_adj_image(base_img):
            returndict = {}
            # Brightness adjustments
            for factor in [0.9, 1.1]:
                enhancer = ImageEnhance.Brightness(base_img)
                returndict[f"brightness_{factor}x"] = enhancer.enhance(factor)
            return returndict

        def contrast_adj_image(base_img):
            returndict = {}
            # Contrast adjustments
            for factor in [0.9, 1.1]:
                enhancer = ImageEnhance.Contrast(base_img)
                returndict[f"contrast_{factor}x"] = enhancer.enhance(factor)
            return returndict

        def simulate_compression(base_img):
            return_dict = {}
            with io.BytesIO() as temp_buffer:
                base_img.save(temp_buffer, format="JPEG", quality=85)
                temp_buffer.seek(0)
                jpeg_img = Image.open(temp_buffer)
                # Ensure consistent format & size
                return_dict["jpeg_compression"] = jpeg_img.convert("L").resize(self.resize)
            return return_dict


        mode = self.mode
        permutations = {}
        base_img = Image.open(image_path).resize(self.resize).convert("L")
        
        # Base perms
        h_flip = base_img.transpose(Image.FLIP_LEFT_RIGHT)
        v_flip = base_img.transpose(Image.FLIP_TOP_BOTTOM)
        permutations['original'] = base_img
        permutations['H_Flip'] = h_flip
        permutations['v_Flip'] = v_flip

        # Advanced perms
        if mode == 'advanced':
            rotations = rotated_image(base_img)
            permutations.update(rotations)
            zoomed = zoomed_image(base_img, zoom_amount=0.05)
            permutations.update(zoomed)
            compressed = simulate_compression(base_img)
            permutations.update(compressed)
            brightened = brightness_adj_image(base_img)
            permutations.update(brightened)
            contrast_adj = contrast_adj_image(base_img)
            permutations.update(contrast_adj)

        return permutations

    def generate_hashes_and_bytes(self, image_path) -> dict:
        """
        Generates the hash-keys and bytes for a given image, requires calling of generate_permutations to generate image variations

        Args:
            image_path: path to the image, to be passed into generate_permutations to generate variations.

        Returns:
            result: dictionary of image, image_path, hashes and bytes.
        """
        # Define 'result' dictionary to store the image and image path alongside its hashes & bytes
        result = {}
        filename = os.path.basename(image_path)
        result["filename"] = filename
        result["filepath"] = image_path

        permutations = self.generate_permutations(image_path)
        for key, img in permutations.items():
            # Compute average hash (convert the result to string)
            img_hash = str(imagehash.average_hash(img))
            result[f"{key}_hash"] = img_hash
            # Convert image to bytes (PNG format)
            result[f"{key}_bytes"] = self.image_to_bytes(img)

        return result

    def process_image_directory(self, folder_path) -> list:
        """
        Process all images in a folder, generating a dictionary for each.
        Only files with extensions in image_extensions are processed.

        Returns:
            list of dictionaries of hashes and bytes of each image in directory.
        """
        image_extensions = ('.png', '.jpg', '.jpeg', '.bmp', '.gif')
        results = []
        for file in os.listdir(folder_path):
            if file.lower().endswith(image_extensions):
                image_path = os.path.join(folder_path, file)
                data = self.generate_hashes_and_bytes(image_path)
                results.append(data)
        return results  
        

class ImageScanner:
    """
    ImageScanner compares a dataframe of hashes & bytes to an existing database of hashes and bytes,
    returning matching values via various functions.
    """

    def __init__(self, _database: pd.DataFrame):
        """
        Initializes ImageScanner with an existing image database (as a DataFrame).
        """
        self.database = _database  # stored image data as a DataFrame
        self.matches_df = pd.DataFrame()  # DataFrame to store the match results

    @staticmethod
    def bytes_to_image(image_bytes):
        return Image.open(io.BytesIO(image_bytes))

    def batch_test_hashes_to_df(self, batch_test_hashes: list) -> pd.DataFrame:
        """
        Converts a list of hash dictionaries (each representing an image) into a pandas DataFrame.

        Args:
            batch_test_hashes (list): List of dictionaries with image hash data.

        Returns:
            pd.DataFrame: DataFrame constructed from the list.
        """
        return pd.DataFrame(batch_test_hashes)

    def compare_hashes_fuzzy_crosswise_df(self, test_df: pd.DataFrame, threshold: int = 5) -> pd.DataFrame:
        """
        Compares two DataFrames in a crosswise manner: one for test images and one from the database.
        For each test image, every hash (column ending with '_hash') is compared with every hash in the database.
        Matches are recorded if the Hamming distance is 0 (Exact) or within the specified threshold (Fuzzy).

        Args:
            test_df (pd.DataFrame): DataFrame containing test image hash dictionaries.
            threshold (int): Maximum Hamming distance to consider a fuzzy match.

        Returns:
            pd.DataFrame: A DataFrame containing match details with columns for test image, database image,
                          the hash keys compared, the type of match, and the computed distance.
        """
        # Identify the columns that contain hash values in each DataFrame.
        test_hash_cols = [col for col in test_df.columns if col.endswith('_hash')]
        db_hash_cols = [col for col in self.database.columns if col.endswith('_hash')]

        records = []  # List to store match record dictionaries.

        # Iterate over each test image (row) in the test DataFrame.
        for _, test_row in test_df.iterrows():
            test_filename = test_row.get('filename', 'Unknown')
            for test_col in test_hash_cols:
                test_hash_str = test_row[test_col]
                try:
                    test_hash_obj = imagehash.hex_to_hash(test_hash_str)
                except Exception:
                    continue  # Skip if conversion fails.
                # For each stored image in the database.
                for _, db_row in self.database.iterrows():
                    db_filename = db_row.get('filename', 'Unknown')
                    for db_col in db_hash_cols:
                        db_hash_str = db_row[db_col]
                        try:
                            db_hash_obj = imagehash.hex_to_hash(db_hash_str)
                        except Exception:
                            continue  # Skip if conversion fails.
                        distance = test_hash_obj - db_hash_obj  # Compute Hamming distance.
                        normalized_distance = distance / len(test_hash_obj.hash) ** 2
                        similarity_score = 1 - normalized_distance
                        if distance == 0:
                            records.append({
                                'test_filename': test_filename,
                                'db_filename': db_filename,
                                'test_hash_key': test_col,
                                'db_hash_key': db_col,
                                'match_level': 'Exact',
                                'distance': distance,
                                'similarity': 1 - (distance / len(test_hash_obj.hash) ** 2)
                            })
                        elif distance <= threshold:
                            records.append({
                                'test_filename': test_filename,
                                'db_filename': db_filename,
                                'test_hash_key': test_col,
                                'db_hash_key': db_col,
                                'match_level': f'Fuzzy, Distance={distance}',
                                'distance': distance,
                                'similarity': 1 - (distance / len(test_hash_obj.hash) ** 2)
                            })

        # Convert the records list into a DataFrame.
        matches_df = pd.DataFrame(records)
        self.matches_df = matches_df
        return matches_df

    def print_matches_df(self):
        """
        Prints the matches DataFrame in a readable format.
        """
        if not self.matches_df.empty:
            print(self.matches_df)
        else:
            print("No matches found.")
