def main(testing=False):
    config_path = "config/config.yaml"
    if testing:
        print("[INFO] Running in test mode...")
        config_path = "config/config_test.yaml"
    # load config, run processing
    ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing", action="store_true")
    args = parser.parse_args()
    main(testing=args.testing)