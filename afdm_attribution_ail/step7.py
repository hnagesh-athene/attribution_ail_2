"""
transformation step-7
"""

class Step7:
    """
    changes to be made in step 7
    """

    def __init__(self):
        """
        define fields to be modified in step 1
        """
        print("Step 7 class")
        self.functions = [self.seed]

    def seed(self, merger_row, current_row, fieldnames):
        """
        logic for the field
        """
        if "Seed" in fieldnames:
            if merger_row['join_indicator'] == 'AB' and merger_row["IRRestartNew_PQ"] == merger_row["IRRestartNew_CQ"]\
             and merger_row["IRRestartNew_CQ"] not in ("_", "U"):
                current_row["Seed"] = merger_row["Seed_PQ"]
        return current_row
