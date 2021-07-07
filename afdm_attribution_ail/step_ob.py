class OB:
    """
    changes to be made in step 1
    """

    def __init__(self):
        """
        define fields to be modified in step 1
        """
        print("other blocks")
        self.functions = [
            self.GenBudgetOBCurr,
            self.GenBudgetUltOB,
            self.generate,
            self.fixed,
        ]

    def GenBudgetOBCurr(self, merger_row, current_row, fieldnames):

        if "GenBudgetOBCurr" in fieldnames:
            if merger_row["join_indicator"] == "AB":
                current_row["GenBudgetOBCurr"] = merger_row["GenBudgetOBCurr_PQ"]
            else:
                current_row["GenBudgetOBCurr"] = merger_row["GenBudgetOBCurr_CQ"]
        return current_row

    def GenBudgetUltOB(self, merger_row, current_row, fieldnames):

        if "GenBudgetUltOB" in fieldnames:
            if merger_row["join_indicator"] == "AB":
                current_row["GenBudgetUltOB"] = merger_row["GenBudgetUltOB_PQ"]
            else:
                current_row["GenBudgetUltOB"] = merger_row["GenBudgetUltOB_CQ"]
        return current_row

    def fixed(self, merger_row, current_row, fieldnames):
        """
        populating fields with fixed strategies with default value
        """
        if merger_row["join_indicator"] == "AB" and float(merger_row["F133AVIF_PQ"]) > 0 \
                and float(merger_row["F133AVIF_CQ"]) == 0:
            if "Idx1BudgetUltOB" in fieldnames:
                for i in range(1, 6):
                    current_row[f"Idx{i}BudgetUltOB"] = 0
            if "Idx1BudgetOBCurr" in fieldnames:
                for i in range(1, 6):
                    current_row[f"Idx{i}BudgetOBCurr"] = 0
            if "Idx1BudgetVolAdjOB" in fieldnames:
                for i in range(1, 6):
                    current_row[f"Idx{i}BudgetVolAdjOB"] = 0
            if "Idx1AOptNomMV" in fieldnames:
                for i in range(1, 6):
                    current_row[f"Idx{i}AOptNomMV"] = 0
        return current_row

    def generate(self, merge, current_row, fieldnames):
        """
        Default fields
        """
        for fields in fieldnames:
            if fields in ("PolNo", "Company"):
                current_row[fields] = merge[fields]
            elif fields not in ("GenBudgetOBCurr", "GenBudgetUltOB"):
                current_row[fields] = merge[fields + "_CQ"]
        return current_row
