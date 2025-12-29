"""
Client enum definitions and deterministic color generation utilities.

This module defines supported client companies and provides a deterministic
color generation mechanism using OKLCH color space. Colors are generated
in a way that ensures good contrast and avoids duplicates across clients.
"""

from enum import Enum
import hashlib
import math
class Company(Enum):
    """
    Enumeration of supported client companies.
    """
    ALASKA_COMMUNICATIONS="Alaska Communications Systems Holdings Inc"
    YMCA="National Council of Young Men's Christian Association of the USA of America"
    MOURITECH_LLC="MOURI Tech LLC"
    ILC_DOVER="ILC Dover"
    LAZARD="Lazard Freres & Co LLC"
    MOURITECH="MOURI Tech Limited"
    CHEP="CHEP USA Inc"
    HUDSON="Hudson Advisors L.P"
    NAT_GRID="National Grid USA Service Company Inc"
    LEASELOCK="LeaseLock Inc"
    DZS="DZS Inc"
    SOUL_CYCLE="SoulCycle Inc"
    VERTISYSTEMS="Vertisystem Inc"
    CLAIR_SOURCE="Clair Source Group"
    ATD="American Tire Distributors Inc"
    SVI="Storage Vault Canada Inc"
    TRINET="TriNet USA Inc"
    HFT="Harbor Freight Tools USA Inc"
    ICON="Citizens Icon Holdings LLC"
    VERTEXONE="VERTEXONE SOFTWARE LLC"
    EBOS="Symbion Pty Ltd (EBOS)"
    HARRIS_FARM="Harris Farm Markets Pty Ltd"
    GLOBUS="Globus Medical Inc"
    DELOITTE="Deloitte Consulting India Private Limited"
    FREEMAN="Freeman Corporation"
    SWIFT="Swift Beef Company"
    GCG="Goddard Catering Group"
    SEPHORA="Sephora"
    DEVFI="Devfi Inc"
    LACTALIS="LACTALIS AUSTRALIA PTY LTD"
    ENERSYS="EnerSys Delaware Inc"
    LENOVO="Lenovo PC HK LTD"
    HUMMING_BIRD="Humming Bird Education Limited"
    NWN="NWN Corporation"
    RITCHIE="Ritchie Bros. Auctioneers Inc"
    INGERSOLL_RAND="Ingersoll Rand"
    BINGO="Bingo Industries"
    ESTABLISHMENT_LABS="Establishment Labs SA"
    ONESTREAM="OneStream Software LLC"
    EQUINOX="Equinox Holdings Inc"
    STRADA="Strada U.S. Payroll LLC"
    SIPEF="SIPEF Singapore Pte Ltd"
    SUNTEX="Suntex Marinas LLC"
    SAMSARA="Samsara Inc"
    SIGNIA="Signia Aerospace"
    ATNI="ATN International Services LLC"
    ANICA="Anica Inc"
    ELF="ELF Cosmetics Inc"
    TOYOTA="Toyota Canada Inc"
    REGAL="Regal Rexnord Corporation"
    UWF="University of Wisconsin Foundation"
    DELEK="Delek US Holdings Inc"



def _oklch_to_hex(L_pct: float, C: float, h: float) -> str:
    """
    Convert OKLCH color values to a HEX color string.

    Args:
        lightness_pct (float): Lightness percentage (0–100).
        chroma (float): Chroma value.
        hue (float): Hue angle in degrees.

    Returns:
        str: HEX color string.
    """
    L = L_pct / 100.0
    a = C * math.cos(math.radians(h))
    b = C * math.sin(math.radians(h))

    l_ = L + 0.3963377774 * a + 0.2158037573 * b
    m_ = L - 0.1055613458 * a - 0.0638541728 * b
    s_ = L - 0.0894841775 * a - 1.2914855480 * b

    l = l_ ** 3
    m = m_ ** 3
    s = s_ ** 3

    r = +4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s
    g = -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s
    b = -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s

    # linear → sRGB
    def to_srgb(x: float) -> float:
        if x <= 0.0031308:
            return 12.92 * x
        return 1.055 * (x ** (1 / 2.4)) - 0.055

    r = min(max(to_srgb(r), 0.0), 1.0)
    g = min(max(to_srgb(g), 0.0), 1.0)
    b = min(max(to_srgb(b), 0.0), 1.0)

    return f"#{int(r*255):02X}{int(g*255):02X}{int(b*255):02X}"



PALETTE = [
    (63.7, 0.237, 25.331),
    (75.0, 0.183, 55.934),
    (84.1, 0.238, 128.85),
    (78.9, 0.154, 211.53),
    (62.3, 0.214, 259.815),
    (74.0, 0.238, 322.16),
    (65.6, 0.241, 354.308),
    (87.2, 0.01, 258.338),
    (91.7, 0.08, 205.041),
    (95.4, 0.038, 75.164),
    (97.3, 0.071, 103.193),
]



def generate_unique_colors(enum_cls):
    """
    Generate unique HEX colors for enum members.

    Colors are deterministically derived from enum names and adjusted
    to avoid duplicates while maintaining contrast.

    Args:
        enum_cls (Enum): Enum class to generate colors for.

    Returns:
        dict: Mapping of enum member to HEX color string.
    """
    color_map = {}
    used = set()

    for company in enum_cls:
        digest = int(
            hashlib.sha256(company.name.encode()).hexdigest(),
            16,
        )

        base_L, C, h = PALETTE[digest % len(PALETTE)]
        L = base_L
        step = 0

        while True:
            color = _oklch_to_hex(L, C, h)

            if color not in used:
                used.add(color)
                color_map[company] = color
                break
            step += 1
            delta = 2.5 * step
            L = max(55.0, min(90.0, base_L - delta if step % 2 else base_L + delta))

    return color_map
