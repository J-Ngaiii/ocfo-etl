import os
import sys

import sys
print(f"Sys path: {sys.path}")

from AEOCFO.Transform import ABSA_Processor, Agenda_Processor, OASIS_Abridged
import AEOCFO.Utility.Cleaning as cl
import AEOCFO.Utility.Utils as ut

print(f"Utils Func: {ut.heading_finder}")
print(f"Cleaning Func: {cl.is_type}")
print(f"{ABSA_Processor.__name__}: imported successfully!")
print(f"{Agenda_Processor.__name__}: imported successfully!")
print(f"{OASIS_Abridged.__name__}: imported successfully!")

print("All print statements completed, pathing functional!")
