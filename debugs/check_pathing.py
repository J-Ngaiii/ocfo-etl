import os
import sys

import sys
print(f"Sys path: {sys.path}")

from AEOCFO.Transform import ABSA_Processor, Agenda_Processor, OASIS_Abridged, FR_ProcessorV2, process_weekly_pipeline
import AEOCFO.Utility.Cleaning as cl
import AEOCFO.Utility.Utils as ut

print(f"Utils Func: {ut.heading_finder}")
print(f"Cleaning Func: {cl.is_type}")

functions = [ABSA_Processor, Agenda_Processor, OASIS_Abridged, FR_ProcessorV2, process_weekly_pipeline]
for func in functions:
    print(f"{func.__name__}: imported successfully!")

print("All print statements completed, pathing functional!")
