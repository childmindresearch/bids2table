from pathlib import Path

import nibabel as nib
import numpy as np
import pytest

from bids2table.extractors.image import extract_image_meta


@pytest.fixture
def dummy_image(tmp_path: Path) -> Path:
    img_path = tmp_path / "img.nii.gz"
    data = np.random.randn(50, 50, 50, 10).astype("float32")
    data = np.asfortranarray(data)
    img = nib.Nifti1Image(data, np.eye(4))
    img.to_filename(img_path)
    return img_path


@pytest.mark.parametrize("backend", ["nibabel", "nifti"])
def test_extract_image_meta(dummy_image: Path, backend: str):
    image_meta = extract_image_meta(dummy_image, backend=backend)
    assert list(image_meta.keys()) == ["image_header", "image_affine"]
    img_dim = image_meta["image_header"]["dim"]
    assert img_dim[1:5] == [50, 50, 50, 10]


if __name__ == "__main__":
    pytest.main([__file__])
