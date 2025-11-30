import datashader as ds
import pandas as pd
import datashader.transfer_functions as tf
import numpy as np

def verify_width_071():
    width = 1.0 / np.sqrt(2)
    print(f"Testing line_width = {width:.4f}")
    
    # 1. Diagonal (-1, -1) to (1, 1)
    df_diag = pd.DataFrame({'x': [-1, 1], 'y': [-1, 1]})
    cvs = ds.Canvas(plot_width=3, plot_height=3, x_range=(-1.0, 1.0), y_range=(-1.0, 1.0))
    agg_diag = cvs.line(df_diag, 'x', 'y', line_width=width)
    
    print("\nDiagonal (Center Pixel):")
    # Center pixel is at index [1, 1]
    # Data is bottom-up, so row 1 is middle.
    print(f"{agg_diag.values[1, 1]:.4f}")
    
    # 2. Horizontal (-1, 0) to (1, 0) (Centered)
    df_horz = pd.DataFrame({'x': [-1, 1], 'y': [0, 0]})
    agg_horz = cvs.line(df_horz, 'x', 'y', line_width=width)
    
    print("\nHorizontal (Center Pixel):")
    print(f"{agg_horz.values[1, 1]:.4f}")

if __name__ == "__main__":
    verify_width_071()
