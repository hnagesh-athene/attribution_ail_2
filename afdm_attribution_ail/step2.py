"""
transformation step-2
"""


class Step2:
    """
    changes to be made in step 2
    """

    def __init__(self):
        """
        define fields to be modified in step 1
        """
        print("Step 2 class")
        self.functions = [self.ICOSFlag]

    def ICOSFlag(self, merger_row, previous_row, fieldnames):
        """
        logic for the field
        """
        if "ICOSFlag" in fieldnames:
            if merger_row["join_indicator"] == "AB":
                previous_row["ICOSFlag"] = merger_row["ICOSFlag_CQ"]
        return previous_row
