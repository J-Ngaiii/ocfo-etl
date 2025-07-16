from AEOCFO.Pipeline.Execute import main

import argparse

if __name__ == "__main__":
    process = 'FR'

    # because doing --arg True/False we do (no flag) --> true vs (flag) --> False
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing", action="store_true", help=f"Run in testing mode for {process}")
    parser.add_argument("--no-verbose", dest="verbose", action="store_false", help="Disable verbose logging")
    parser.add_argument("--no-drive", dest="drive", action="store_false", help="Disable Google Drive processing")
    parser.add_argument("--no-bigquery", dest="bigquery", action="store_false", help="Disable BigQuery push")

    parser.set_defaults(verbose=True, drive=True, bigquery=True, testing=False)
    args = parser.parse_args()

    main(
        t=process, 
        verbose=args.verbose, 
        drive=args.drive, 
        bigquery=args.bigquery, 
        testing=args.testing
    )
    
    