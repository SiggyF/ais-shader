import datashader as ds
import pandas as pd
import datashader.transfer_functions as tf
import numpy as np

def verify_matrix():
    # Define a line from bottom-left (-1, -1) to top-right (1, 1)
    df = pd.DataFrame({'x': [-1, 1], 'y': [-1, 1]})
    
    # Create a 3x3 canvas covering the domain
    cvs = ds.Canvas(plot_width=3, plot_height=3, x_range=(-1.0, 1.0), y_range=(-1.0, 1.0))
    
    # Render line with line_width=1
    agg = cvs.line(df, 'x', 'y', line_width=1)
    
    print("Datashader Output (3x3, line_width=1):")
    data = agg.values
    for row in data[::-1]:
        print([f"{x:.4f}" for x in row])

if __name__ == "__main__":
    verify_matrix()
