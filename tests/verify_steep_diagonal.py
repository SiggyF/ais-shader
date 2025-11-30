import datashader as ds
import pandas as pd
import datashader.transfer_functions as tf
import numpy as np

def verify_steep():
    # Define a line from (-1, -1) to (0, 1)
    df = pd.DataFrame({'x': [-1, 0], 'y': [-1, 1]})
    
    # Create a 3x3 canvas covering the domain
    cvs = ds.Canvas(plot_width=3, plot_height=3, x_range=(-1.0, 1.0), y_range=(-1.0, 1.0))
    
    # Render line with line_width=1
    agg = cvs.line(df, 'x', 'y', line_width=1)
    
    print("Datashader Output (3x3, line_width=1, Steep Diagonal):")
    data = agg.values
    for row in data[::-1]:
        print([f"{x:.4f}" for x in row])

if __name__ == "__main__":
    verify_steep()
