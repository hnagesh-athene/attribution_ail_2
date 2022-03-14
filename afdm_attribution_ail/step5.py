"""
transformation step-5
"""


class Step5:
    """
    changes to be made in step 5
    """

    def __init__(self):
        """
        define fields to be modified in step 5
        """
        self.fields = [
            "GMWBCharge",
            "GMWBMaxBenTable",
            "GMWBMaxCharge",
            "AdditionalRestartMonths",
            "RestartCharge",
            "Seed",
            "IRRestartNew"
        ]
        self.functions = [self.step_5]

    def step5_functions(self, merger_row, current_row, field):
        """
        logic for the field
        """
        if merger_row["IRRestartNew_PQ"] in ("F", "X") and merger_row["IRRestartNew_CQ"] in ("Y", "N"):
            current_row[f"{field}"] = merger_row[f"{field}" + "_PQ"]

        return current_row

    def step_5(self, merger_row, current_row, fieldnames):

        for field in self.fields:
            if field in fieldnames:
                current_row = self.step5_functions(merger_row, current_row, field)

        return current_row
