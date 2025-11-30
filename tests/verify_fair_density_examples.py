import datashader as ds
import pandas as pd
import datashader.transfer_functions as tf
import numpy as np

def verify_fair_density():
    width = 1.0 / np.sqrt(2)
    print(f"Testing line_width = {width:.4f}")
    
    cvs = ds.Canvas(plot_width=3, plot_height=3, x_range=(-1.0, 1.0), y_range=(-1.0, 1.0))

    # 1. Straight (Horizontal Centered)
    # (-1, 0) to (1, 0)
    df_straight = pd.DataFrame({'x': [-1, 1], 'y': [0, 0]})
    agg_straight = cvs.line(df_straight, 'x', 'y', line_width=width)
    print("\n1. Straight (Horizontal Centered):")
    for row in agg_straight.values[::-1]:
        print([f"{x:.4f}" if not np.isnan(x) else "0.0000" for x in row])

    # 2. Steep Diagonal
    # (-1, -1) to (0, 1)
    df_steep = pd.DataFrame({'x': [-1, 0], 'y': [-1, 1]})
    agg_steep = cvs.line(df_steep, 'x', 'y', line_width=width)
    print("\n2. Steep Diagonal:")
    for row in agg_steep.values[::-1]:
        print([f"{x:.4f}" if not np.isnan(x) else "0.0000" for x in row])

    # 3. Diagonal
    # (-1, -1) to (1, 1)
    df_diag = pd.DataFrame({'x': [-1, 1], 'y': [-1, 1]})
    agg_diag = cvs.line(df_diag, 'x', 'y', line_width=width)
    print("\n3. Diagonal:")
    for row in agg_diag.values[::-1]:
        print([f"{x:.4f}" if not np.isnan(x) else "0.0000" for x in row])

if __name__ == "__main__":
    verify_fair_density()
