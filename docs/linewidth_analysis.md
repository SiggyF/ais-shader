# Line Width Analysis and Recommendations

This document details the impact of `line_width` settings on the rasterization of vessel tracks using Datashader.

## Overview

When rasterizing vector lines into a pixel grid, the `line_width` parameter controls how the line is drawn. This choice involves a trade-off between visual quality (smoothness) and analytical interpretability (counts vs. density).

## 1. Aliased Rendering (`line_width = 0`)

Uses Bresenham's algorithm to select exactly one pixel per step along the major axis.

- **Pros**: Produces **Integer Counts**. If a pixel has a value of 5, exactly 5 vessel tracks passed through it (or near its center). This is the "fairest" way to count *events* (vessel transits).
- **Cons**: Visually jagged. Diagonal lines may appear thinner or disconnected at high zooms compared to anti-aliased lines.
- **Note**: While `line_width=0` counts *transits*, it does not strictly conserve *distance* (a diagonal track of length $\sqrt{2}$ contributes 1 count, whereas an anti-aliased line might distribute $\approx 1.41$ mass). However, for statistical analysis of "how many ships passed here", `line_width=0` is the standard.

### Example: Steep Diagonal (-1, -1) to (0, 1)
```text
+---+---+---+
| 0 | 1 | 0 |
+---+---+---+
| 0 | 1 | 0 |
+---+---+---+
| 1 | 0 | 0 |
+---+---+---+
```

## 2. Anti-Aliased Rendering (`line_width = 1`)

Produces smooth, anti-aliased lines that represent the *spatial coverage* of the vessel track.

- **Pros**: High visual quality, no "jaggies".
- **Cons**: Introduces fractional values (e.g., 0.29) and may saturate (clip) at 1.0 for diagonal lines, potentially over-representing density in terms of pure "pixel mass".

### Example: Diagonal (-1, -1) to (1, 1)
```text
+------+------+------+
| 0.00 | 0.29 | 1.00 |
+------+------+------+
| 0.29 | 1.00 | 0.29 |
+------+------+------+
| 1.00 | 0.29 | 0.00 |
+------+------+------+
```
**Explanation of Values:**
- **`1.00` (Center)**: The diagonal line segment length inside the pixel is $\sqrt{2} \approx 1.41$. Multiplied by the line width ($1$), the total "mass" is $1.41$, which exceeds the pixel area ($1.0$), resulting in a saturated value of `1.00`.
- **`0.29` (Neighbors)**: This value ($1 - 1/\sqrt{2} \approx 0.293$) represents the partial coverage of the adjacent pixels by the 1-unit wide line.

### Example: Steep Diagonal (-1, -1) to (0, 1)
```text
+------+------+------+
| 0.33 | 0.78 | 0.00 |
+------+------+------+
| 0.78 | 0.33 | 0.00 |
+------+------+------+
| 0.78 | 0.00 | 0.00 |
+------+------+------+
```

### Example: Horizontal Top Edge (-1, 1) to (0, 1)
```text
+------+------+------+
| 0.50 | 0.50 | 0.00 |
+------+------+------+
| 0.00 | 0.00 | 0.00 |
+------+------+------+
| 0.00 | 0.00 | 0.00 |
+------+------+------+
```
*(The line runs along the top edge `y=1`, so only half its width is inside the pixel grid, resulting in `0.50` coverage.)*

## 3. Advanced: Mass Conservation (`line_width ≈ 0.71`)

To achieve a density where the accumulated value is strictly proportional to the vessel's travel distance without saturation bias, one can use a fractional line width.

- Use `line_width ≈ 0.71` ($1/\sqrt{2}$).
- **Why?** At `line_width=1`, a diagonal track deposits $\approx 1.41$ mass per pixel, which is clipped to $1.0$, under-representing diagonal movement relative to horizontal movement.
- **Result**: With `line_width=0.71`, the maximum coverage (diagonal) is $1.0$, preventing clipping.
    - **Horizontal**: 1 pixel transit $\rightarrow$ Value 0.71. Total Mass/Length = 0.71.
    - **Diagonal**: $\approx 0.71$ pixel transits per unit length $\rightarrow$ Value 1.0 per pixel. Total Mass/Length = $1.0 \times 0.71 = 0.71$.
    - **Conclusion**: Mass is strictly proportional to length (scaled by 0.71).

### Examples with `line_width ≈ 0.71`

*Straight (Horizontal):*
```text
+--------+--------+--------+
| 0.0000 | 0.0000 | 0.0000 |
+--------+--------+--------+
| 0.7071 | 0.7071 | 0.7071 |
+--------+--------+--------+
| 0.0000 | 0.0000 | 0.0000 |
+--------+--------+--------+
```

*Steep Diagonal:*
```text
+--------+--------+--------+
| 0.2328 | 0.5490 | 0.0000 |
+--------+--------+--------+
| 0.5490 | 0.2328 | 0.0000 |
+--------+--------+--------+
| 0.5490 | 0.0000 | 0.0000 |
+--------+--------+--------+
```

*Diagonal:*
```text
+--------+--------+--------+
| 0.0000 | 0.2071 | 0.7071 |
+--------+--------+--------+
| 0.2071 | 0.7071 | 0.2071 |
+--------+--------+--------+
| 0.7071 | 0.2071 | 0.0000 |
+--------+--------+--------+
```
