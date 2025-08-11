import io
from dataclasses import dataclass
from typing import Dict, Iterable, Literal, Tuple

import imagehash
from PIL import Image, ImageEnhance


HashMode = Literal["basic", "advanced"]


@dataclass
class PermutationConfig:
    resize: Tuple[int, int] = (256, 256)
    mode: HashMode = "basic"


class PermutationGenerator:
    def __init__(self, config: PermutationConfig):
        self.config = config

    @staticmethod
    def _img_to_bytes(img: Image.Image) -> bytes:
        with io.BytesIO() as buf:
            img.save(buf, format="PNG")
            return buf.getvalue()

    def _generate_permutations(self, base_img: Image.Image) -> Dict[str, Image.Image]:
        cfg = self.config
        permutations: Dict[str, Image.Image] = {}

        base = base_img.resize(cfg.resize).convert("L")
        permutations["original"] = base
        permutations["h_flip"] = base.transpose(Image.FLIP_LEFT_RIGHT)
        permutations["v_flip"] = base.transpose(Image.FLIP_TOP_BOTTOM)

        if cfg.mode == "advanced":
            for angle in (-5, 5):
                permutations[f"rot_{angle}"] = base.rotate(angle, resample=Image.BICUBIC, fillcolor=128)

            # Mild zoom crop
            w, h = base.size
            zw, zh = int(w * 0.9), int(h * 0.9)
            left = (w - zw) // 2
            upper = (h - zh) // 2
            zoomed = base.crop((left, upper, left + zw, upper + zh)).resize(cfg.resize, Image.LANCZOS)
            permutations["zoom_10"] = zoomed

            # Slight brightness and contrast tweaks
            for factor in (0.9, 1.1):
                permutations[f"b_{factor}"] = ImageEnhance.Brightness(base).enhance(factor)
                permutations[f"c_{factor}"] = ImageEnhance.Contrast(base).enhance(factor)

        return permutations

    def hashes_for_image(self, image_bytes: bytes) -> Dict[str, str]:
        base_img = Image.open(io.BytesIO(image_bytes))
        perms = self._generate_permutations(base_img)
        result: Dict[str, str] = {}
        for name, img in perms.items():
            result[f"{name}_hash"] = str(imagehash.phash(img))
        return result


