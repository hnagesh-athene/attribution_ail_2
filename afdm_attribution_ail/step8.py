"""
transformation step-8
"""


class Step8:
    """
    changes to be made in step 8
    """

    def __init__(self):
        """
        define fields to be modified in step 8
        """
        self.functions = [self.idxAOptNomMV_formater]

    def idxAOptNomMV(self, merger_row, current_row, index, idx):
        """
        logic for the field
        """
        if merger_row["join_indicator"] == "A":
            current_row[f"Idx{index}AOptNomMV"] = merger_row[f"Idx{index}IncepCost_CQ"]

        if merger_row["join_indicator"] == "AB":
            if merger_row[f"_int_idx{index}_RecLinkID_CQ"] != '_' and merger_row[f"_int_idx{idx}_anniv"] == "N":
                current_row[f"Idx{index}AOptNomMV"] = merger_row[f"Idx{idx}AOptNomMV_PQ"]
            if merger_row[f"_int_idx{index}_RecLinkID_CQ"] != '_' and merger_row[f"_int_idx{idx}_anniv"] == "Y":
                current_row[f"Idx{index}AOptNomMV"] = merger_row[f"Idx{idx}BudgetVolAdjOB_PQ"]
        return current_row

    def idxAOptNomMV_formater(self, merger_row, current_row, fieldnames, args):
        """
        IdxAOptNomMV logic
        """
        for index in range(1, 6):
            if f"Idx{index}AOptNomMV" in fieldnames:
                key = merger_row[f"_int_idx{index}_RecLinkID_CQ"]
                if key != "_" and merger_row["join_indicator"] == "AB":
                    idx = eval(merger_row["__idxordersync_pq"]).get(key, index)
                else:
                    idx = index
                current_row = self.idxAOptNomMV(merger_row, current_row, index, idx)

        return current_row
