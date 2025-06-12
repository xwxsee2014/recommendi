import argparse


class ArgumentParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            description="Question generator for different ND products"
        )
        self.parser.add_argument(
            "--debug",
            "-d",
            default=False,
            help="enable debug mode",
            action="store_true",
        )
        self.parser.add_argument(
            "--module",
            "-m",
            help="choose a runner moduel to run",
            type=str,
            default="",
            action="store",
        )
        self.parser.add_argument(
            "--load_all",
            "-la",
            default=False,
            help="load all modules",
            action="store_true",
        )
        self.parser.add_argument(
            "--eval_slides_info_id",
            "-esid",
            help="slides_info_id for evaluation",
            type=str,
            default="",
            action="store",
        )
        self.parser.add_argument(
            "--eval_compare_targets",
            "-ect",
            help="compare targets for evaluation",
            type=str,
            default="",
            action="store",
        )

    def parse_arguments(self):
        args = self.parser.parse_args()
        return args
