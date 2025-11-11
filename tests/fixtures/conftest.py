import pytest


def pytest_generate_tests(metafunc):
    """
    Parametrize _master_xml_param fixture with XML versions.
    The v9.2R6 version is marked with 'fast' for quick testing.
    Run with -m fast to only test the v9.2R6 version.
    Run without -m fast to test all three XML versions.
    """
    if "_master_xml_param" in metafunc.fixturenames:
        params = [
            pytest.param("v9.2R6", marks=pytest.mark.fast),
            "v10.0R2",
            "v11.0R4",
        ]

        metafunc.parametrize("_master_xml_param", params, indirect=True)
