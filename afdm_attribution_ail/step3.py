"""
transformation step-3
"""

class Step3:
    """
    changes to be made in step 3
    """

    def __init__(self):
        """
        define fields to be modified in step 3
        """
        self.functions = [self.step_3]
        self.fields = [
            "DBRiderCodeYN",
            "RiderCodeYN",
            "ReportingGroup",
            "CohortKey",
            "SOPApply",
            "DBRiderCode",
            "RiderCode",
            "IncRiderAV",
            "PWTable",
            "GMWBCharge",
            "GMWBIncType",
            "GMWBMaxBenTable",
            "GMWBParRate",
            "GMWBParRate2",
            "GMWBParRate3",
            "GMWBParRate4",
            "GMWBRollup",
            "GMWBRollup2",
            "GMWBRollup3",
            "GMWBRollup4",
            "GMWBInitAccumPeriod",
            "GMWBMaxAccumPeriod",
            "GMWBMaxCharge",
            "RiderCharge_DeductGtd",
            "F133IncRiderAV",
            "IRRestartNew",
            "InitialRestartMonths",
            "AdditionalRestartMonths",
            "RestartCharge",
            "Seed",
        ]

    def step_3_functions(self, merger_row, current_row, field):
        """
        logic for the field
        """
        if merger_row['join_indicator'] == 'AB' and ((merger_row["IRRestartNew_PQ"] != "_" and merger_row["IRRestartNew_CQ"] == "_")\
         or (merger_row["DBRiderCodeYN_PQ"] == "Y" and current_row["DBRiderCodeYN"] == "N")):
            current_row[f"{field}"] = merger_row[f"{field}" + "_PQ"]

        return current_row

    def step_3(self, merger_row, current_row, fieldnames):

        for field in self.fields:
            if field in fieldnames:
                current_row = self.step_3_functions(merger_row, current_row, field)

        return current_row
