from AEOCFO.Pipeline.Execute import main

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, help=f"Executes the ETL script for specified dataset type (eg. ABSA, OASIS, FR or Contingency).")
    parser.add_argument("--testing", action="store_true", help=f"Run in testing mode.")
    parser.add_argument("--no-verbose", dest="verbose", action="store_false", help="Disable verbose logging")
    parser.add_argument("--no-drive", dest="drive", action="store_false", help="Disable Google Drive processing")
    parser.add_argument("--no-bigquery", dest="bigquery", action="store_false", help="Disable BigQuery push")

    parser.set_defaults(verbose=True, drive=True, bigquery=True, testing=False)
    args = parser.parse_args()

    main(
        t=args.dataset, 
        verbose=args.verbose, 
        drive=args.drive, 
        bigquery=args.bigquery, 
        testing=args.testing
    )