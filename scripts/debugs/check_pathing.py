import os
import sys

import sys
print(f"Sys path: {sys.path}")

from ASUCExplore.src.Special import ABSA_Processor
import ASUCExplore.src.Cleaning as cl
import ASUCExplore.src.Utils as ut

print(f"Utils Func: {ut.heading_finder}")
print(f"Cleaning Func: {cl.get_valid_iter}")
print(f"ABSA Processor: {ABSA_Processor}")

print("All print statements completed, pathing functional!")
