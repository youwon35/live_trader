from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "assets"
PNG_PATH = ASSETS / "app-icon.png"
ICO_PATH = ASSETS / "app-icon.ico"
ICO_SIZES = (16, 24, 32, 48, 64, 128, 256)


Color = tuple[int, int, int, int]


def clamp(value: float) -> int:
    return max(0, min(255, int(round(value))))


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def mix(c1: Color, c2: Color, t: float) -> Color:
    return (
        clamp(lerp(c1[0], c2[0], t)),
        clamp(lerp(c1[1], c2[1], t)),
        clamp(lerp(c1[2], c2[2], t)),
        clamp(lerp(c1[3], c2[3], t)),
    )


def blend(dst: Color, src: Color) -> Color:
    alpha = src[3] / 255
    inv = 1 - alpha
    return (
        clamp(src[0] * alpha + dst[0] * inv),
        clamp(src[1] * alpha + dst[1] * inv),
        clamp(src[2] * alpha + dst[2] * inv),
        clamp(255 * (alpha + dst[3] / 255 * inv)),
    )


def in_round_rect(x: float, y: float, left: float, top: float, right: float, bottom: float, radius: float) -> bool:
    if x < left or x >= right or y < top or y >= bottom:
        return False
    cx = min(max(x, left + radius), right - radius - 1)
    cy = min(max(y, top + radius), bottom - radius - 1)
    return (x - cx) ** 2 + (y - cy) ** 2 <= radius**2


def set_px(pixels: list[Color], size: int, x: int, y: int, color: Color) -> None:
    if 0 <= x < size and 0 <= y < size:
        idx = y * size + x
        pixels[idx] = blend(pixels[idx], color)


def draw_round_rect(pixels: list[Color], size: int, box: tuple[float, float, float, float], radius: float, color: Color) -> None:
    left, top, right, bottom = box
    for y in range(math.floor(top), math.ceil(bottom)):
        for x in range(math.floor(left), math.ceil(right)):
            if in_round_rect(x + 0.5, y + 0.5, left, top, right, bottom, radius):
                set_px(pixels, size, x, y, color)


def draw_line(pixels: list[Color], size: int, p1: tuple[float, float], p2: tuple[float, float], width: float, color: Color) -> None:
    x1, y1 = p1
    x2, y2 = p2
    dx = x2 - x1
    dy = y2 - y1
    length_sq = dx * dx + dy * dy
    radius = width / 2
    pad = math.ceil(radius + 1)
    min_x = max(0, math.floor(min(x1, x2) - pad))
    max_x = min(size - 1, math.ceil(max(x1, x2) + pad))
    min_y = max(0, math.floor(min(y1, y2) - pad))
    max_y = min(size - 1, math.ceil(max(y1, y2) + pad))
    for y in range(min_y, max_y + 1):
        for x in range(min_x, max_x + 1):
            if length_sq == 0:
                dist = math.hypot(x + 0.5 - x1, y + 0.5 - y1)
            else:
                t = max(0, min(1, ((x + 0.5 - x1) * dx + (y + 0.5 - y1) * dy) / length_sq))
                px = x1 + t * dx
                py = y1 + t * dy
                dist = math.hypot(x + 0.5 - px, y + 0.5 - py)
            if dist <= radius:
                set_px(pixels, size, x, y, color)


def draw_circle(pixels: list[Color], size: int, center: tuple[float, float], radius: float, color: Color) -> None:
    cx, cy = center
    for y in range(max(0, math.floor(cy - radius)), min(size, math.ceil(cy + radius))):
        for x in range(max(0, math.floor(cx - radius)), min(size, math.ceil(cx + radius))):
            if math.hypot(x + 0.5 - cx, y + 0.5 - cy) <= radius:
                set_px(pixels, size, x, y, color)


def draw_polyline(pixels: list[Color], size: int, points: list[tuple[float, float]], width: float) -> None:
    blue = (68, 155, 255, 255)
    green = (39, 209, 127, 255)
    for index, (start, end) in enumerate(zip(points, points[1:])):
        draw_line(pixels, size, start, end, width, mix(blue, green, (index + 0.5) / max(1, len(points) - 1)))


def render_icon(size: int) -> list[Color]:
    pixels: list[Color] = [(0, 0, 0, 0)] * (size * size)
    radius = size * 0.22
    for y in range(size):
        for x in range(size):
            if in_round_rect(x + 0.5, y + 0.5, 0, 0, size, size, radius):
                t = (x + y) / (2 * size)
                pixels[y * size + x] = mix((7, 17, 31, 255), (16, 35, 58, 255), t)

    border = max(1, size * 0.025)
    draw_round_rect(pixels, size, (size * 0.05, size * 0.05, size * 0.95, size * 0.95), size * 0.17, (37, 99, 235, 70))
    draw_round_rect(pixels, size, (size * 0.08, size * 0.08, size * 0.92, size * 0.92), size * 0.145, (7, 17, 31, 90))

    shield = [
        (size * 0.50, size * 0.18),
        (size * 0.72, size * 0.27),
        (size * 0.72, size * 0.45),
        (size * 0.50, size * 0.72),
        (size * 0.28, size * 0.45),
        (size * 0.28, size * 0.27),
        (size * 0.50, size * 0.18),
    ]
    for start, end in zip(shield, shield[1:]):
        draw_line(pixels, size, start, end, max(2, size * 0.045), (90, 166, 255, 235))
    draw_round_rect(pixels, size, (size * 0.32, size * 0.31, size * 0.68, size * 0.66), size * 0.055, (12, 31, 52, 175))

    points = [(size * 0.26, size * 0.62), (size * 0.41, size * 0.48), (size * 0.54, size * 0.56), (size * 0.76, size * 0.31)]
    draw_polyline(pixels, size, points, max(3, size * 0.065))
    draw_line(pixels, size, (size * 0.69, size * 0.31), (size * 0.76, size * 0.31), max(3, size * 0.055), (39, 209, 127, 255))
    draw_line(pixels, size, (size * 0.76, size * 0.31), (size * 0.76, size * 0.39), max(3, size * 0.055), (39, 209, 127, 255))

    for point in points[:-1]:
        draw_circle(pixels, size, point, max(2, size * 0.035), (139, 194, 255, 245))

    if size >= 32:
        draw_line(pixels, size, (size * 0.25, size * 0.28), (size * 0.25, size * 0.40), max(2, size * 0.035), (255, 107, 107, 230))
        draw_line(pixels, size, (size * 0.21, size * 0.34), (size * 0.29, size * 0.34), max(2, size * 0.035), (255, 107, 107, 230))

    # Restore a crisp outer edge after drawing broad translucent fills.
    inner = size * 0.02
    for y in range(size):
        for x in range(size):
            if in_round_rect(x + 0.5, y + 0.5, inner, inner, size - inner, size - inner, radius * 0.92):
                continue
            pixels[y * size + x] = (0, 0, 0, 0)
    _ = border
    return pixels


def png_chunk(name: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + name + data + struct.pack(">I", zlib.crc32(name + data) & 0xFFFFFFFF)


def write_png(path: Path, size: int, pixels: list[Color]) -> None:
    rows = []
    for y in range(size):
        row = bytearray([0])
        for r, g, b, a in pixels[y * size : (y + 1) * size]:
            row.extend((r, g, b, a))
        rows.append(bytes(row))
    payload = b"".join(rows)
    data = b"\x89PNG\r\n\x1a\n"
    data += png_chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0))
    data += png_chunk(b"IDAT", zlib.compress(payload, 9))
    data += png_chunk(b"IEND", b"")
    path.write_bytes(data)


def dib_for_ico(size: int, pixels: list[Color]) -> bytes:
    pixel_bytes = bytearray()
    for y in range(size - 1, -1, -1):
        for r, g, b, a in pixels[y * size : (y + 1) * size]:
            pixel_bytes.extend((b, g, r, a))
    mask_stride = ((size + 31) // 32) * 4
    and_mask = b"\x00" * (mask_stride * size)
    header = struct.pack("<IIIHHIIIIII", 40, size, size * 2, 1, 32, 0, len(pixel_bytes), 0, 0, 0, 0)
    return header + bytes(pixel_bytes) + and_mask


def write_ico(path: Path) -> None:
    images = [(size, dib_for_ico(size, render_icon(size))) for size in ICO_SIZES]
    directory_size = 6 + 16 * len(images)
    offset = directory_size
    entries = bytearray()
    payload = bytearray()
    for size, data in images:
        entries.extend(
            struct.pack(
                "<BBBBHHII",
                0 if size == 256 else size,
                0 if size == 256 else size,
                0,
                0,
                1,
                32,
                len(data),
                offset,
            )
        )
        payload.extend(data)
        offset += len(data)
    path.write_bytes(struct.pack("<HHH", 0, 1, len(images)) + bytes(entries) + bytes(payload))


def main() -> None:
    ASSETS.mkdir(parents=True, exist_ok=True)
    write_png(PNG_PATH, 256, render_icon(256))
    write_ico(ICO_PATH)
    print(f"Wrote {PNG_PATH}")
    print(f"Wrote {ICO_PATH}")


if __name__ == "__main__":
    main()
