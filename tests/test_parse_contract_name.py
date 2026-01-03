# tests/test_load_contracts.py
import sys
from pathlib import Path

# ensure src/ is on sys.path so we can import the package without installing
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from python.parse_contract_name import parse_contract_filename


def test_parse_contract_filename_valid():
    assert parse_contract_filename("ADF18.csv") == ("AD", "F", 2018)


def test_parse_contract_filename_invalid():
    assert parse_contract_filename("CONTINUOUS") is None