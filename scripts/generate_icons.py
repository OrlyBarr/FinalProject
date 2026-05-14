#!/usr/bin/env python3
"""
generate_icons.py — Generate PWA icons for "Israel Transit — גוש דן".

Creates the following PNGs in <project_root>/icons/:
  icon-192.png            (192x192, standard "any")
  icon-512.png            (512x512, standard "any")
  icon-maskable-512.png   (512x512, "maskable" — safe-area padded for adaptive icons)
  apple-touch-icon.png    (180x180, iOS home-screen icon)

Design: a teal rounded square with a white bus glyph and a small location pin,
matching the existing moovit.html palette (primary #00B1D2, primary-dark #0090B0).

Run:
    python scripts/generate_icons.py
The user can replace these PNGs with hand-designed art at any time —
filenames just need to stay the same.
"""

from __future__ import annotations
import os
from pathlib import Path
from PIL import Image, ImageDraw

# ── Palette (matches CSS vars in moovit.html) ───────────────────────────
PRIMARY      = (0, 177, 210)      # #00B1D2
PRIMARY_DARK = (0, 144, 176)      # #0090B0
WHITE        = (255, 255, 255)
ACCENT       = (255, 235, 59)     # warm headlight
WINDOW_BLUE  = (200, 240, 250)    # bus windows
SHADOW       = (0, 0, 0, 50)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ICON_DIR = PROJECT_ROOT / "icons"


def _draw_bus(draw: ImageDraw.ImageDraw, cx: int, cy: int, scale: float) -> None:
    """Draw a stylized bus centered at (cx, cy) with the given scale (1.0 = 256px)."""
    w = int(180 * scale)
    h = int(120 * scale)
    x0, y0 = cx - w // 2, cy - h // 2
    x1, y1 = x0 + w, y0 + h
    r = int(22 * scale)

    # Body
    draw.rounded_rectangle((x0, y0, x1, y1), radius=r, fill=WHITE)

    # Top windshield strip (slightly tinted)
    win_h = int(h * 0.42)
    draw.rounded_rectangle(
        (x0 + int(10 * scale), y0 + int(10 * scale),
         x1 - int(10 * scale), y0 + win_h),
        radius=int(12 * scale),
        fill=WINDOW_BLUE,
    )

    # Window dividers (3 windows)
    win_top = y0 + int(14 * scale)
    win_bot = y0 + win_h - int(4 * scale)
    seg_w = (w - int(20 * scale)) // 3
    for i in range(1, 3):
        xline = x0 + int(10 * scale) + seg_w * i
        draw.line((xline, win_top, xline, win_bot), fill=PRIMARY, width=max(2, int(3 * scale)))

    # Headlight
    head_r = int(7 * scale)
    draw.ellipse(
        (x1 - int(20 * scale), y0 + win_h + int(8 * scale),
         x1 - int(20 * scale) + head_r * 2, y0 + win_h + int(8 * scale) + head_r * 2),
        fill=ACCENT,
    )

    # Door line
    door_x = x0 + int(28 * scale)
    draw.line(
        (door_x, y0 + win_h + int(4 * scale), door_x, y1 - int(18 * scale)),
        fill=PRIMARY, width=max(2, int(3 * scale)),
    )

    # Wheels
    wheel_r = int(16 * scale)
    wy = y1 - int(4 * scale)
    for wx in (x0 + int(28 * scale), x1 - int(28 * scale) - wheel_r * 2):
        draw.ellipse(
            (wx, wy - wheel_r, wx + wheel_r * 2, wy + wheel_r),
            fill=PRIMARY_DARK,
        )
        draw.ellipse(
            (wx + int(5 * scale), wy - wheel_r + int(5 * scale),
             wx + wheel_r * 2 - int(5 * scale), wy + wheel_r - int(5 * scale)),
            fill=WHITE,
        )


def _draw_pin(draw: ImageDraw.ImageDraw, cx: int, cy: int, scale: float) -> None:
    """Small location pin in the corner."""
    r = int(18 * scale)
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=PRIMARY_DARK, outline=WHITE, width=max(2, int(3 * scale)))
    draw.ellipse(
        (cx - r // 2, cy - r // 2, cx + r // 2, cy + r // 2),
        fill=WHITE,
    )


def make_icon(size: int, *, maskable: bool = False) -> Image.Image:
    """Render one square icon at <size>x<size> pixels."""
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Background — full bleed for "any" icon, padded for "maskable"
    # Maskable requires safe area = 80% center circle; we keep a generous solid
    # background that fills the whole canvas (so a circular mask still shows teal).
    radius = size // 5 if not maskable else 0
    draw.rounded_rectangle(
        (0, 0, size - 1, size - 1),
        radius=radius,
        fill=PRIMARY,
    )

    # Subtle inner gradient via overlapping darker rounded rect at bottom
    bottom = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    bdraw = ImageDraw.Draw(bottom)
    bdraw.rounded_rectangle(
        (0, size // 2, size - 1, size - 1),
        radius=radius if not maskable else 0,
        fill=(*PRIMARY_DARK, 90),
    )
    img = Image.alpha_composite(img, bottom)
    draw = ImageDraw.Draw(img)

    # Safe-area inset for maskable (icon content stays inside 80% center)
    safe = 0.62 if maskable else 0.78
    glyph_scale = (size * safe) / 256.0

    cx = size // 2
    cy = int(size * 0.54)
    _draw_bus(draw, cx, cy, glyph_scale)

    # Corner pin (slightly above bus, centered above-right)
    pin_cx = int(size * 0.68)
    pin_cy = int(size * 0.28)
    if maskable:
        # Keep the pin closer to center for safe area
        pin_cx = int(size * 0.62)
        pin_cy = int(size * 0.32)
    _draw_pin(draw, pin_cx, pin_cy, glyph_scale)

    return img


def main() -> int:
    ICON_DIR.mkdir(parents=True, exist_ok=True)

    targets = [
        ("icon-192.png", 192, False),
        ("icon-512.png", 512, False),
        ("icon-maskable-512.png", 512, True),
        ("apple-touch-icon.png", 180, False),
    ]

    for name, size, maskable in targets:
        img = make_icon(size, maskable=maskable)
        out = ICON_DIR / name
        img.save(out, "PNG", optimize=True)
        print(f"  wrote {out.relative_to(PROJECT_ROOT)}  ({size}x{size}{'  maskable' if maskable else ''})")

    print(f"\nDone. {len(targets)} icons in {ICON_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
