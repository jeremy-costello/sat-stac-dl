import json


class CanadaHierarchy:
    def __init__(self, pr_map="./data/inputs/PR_mapping.json",
                       cd_map="./data/inputs/CD_mapping.json",
                       csd_map="./data/inputs/CSD_mapping.json"):
        # Load mappings once
        with open(pr_map) as f:
            self.prid_to_prname = json.load(f)
        with open(cd_map) as f:
            self.cdid_to_cdname = json.load(f)
        with open(csd_map) as f:
            self.csdid_to_csdname = json.load(f)

    def infer_hierarchy(self, pt):
        """
        Given a sampled point dict, infer the hierarchical IDs and names
        for province, census division, and census subdivision.
        Returns a dictionary with keys: PRUID, PRNAME, CDUID, CDNAME, CSDUID, CSDNAME.
        Missing values are set to None.
        """
        csd_id = pt.get("CSDUID")
        cd_id = pt.get("CDUID")
        pr_id = pt.get("PRUID")

        hierarchy = {
            "PRUID": None,
            "PRNAME": None,
            "CDUID": None,
            "CDNAME": None,
            "CSDUID": None,
            "CSDNAME": None
        }

        if csd_id is not None:
            hierarchy["CSDUID"] = int(csd_id)
            hierarchy["CSDNAME"] = self.csdid_to_csdname.get(str(csd_id))
            cd_id = int(str(csd_id)[:4])
            hierarchy["CDUID"] = cd_id
            hierarchy["CDNAME"] = self.cdid_to_cdname.get(str(cd_id))
            pr_id = int(str(csd_id)[:2])
            hierarchy["PRUID"] = pr_id
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        elif cd_id is not None:
            hierarchy["CDUID"] = int(cd_id)
            hierarchy["CDNAME"] = self.cdid_to_cdname.get(str(cd_id))
            pr_id = int(str(cd_id)[:2])
            hierarchy["PRUID"] = pr_id
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        elif pr_id is not None:
            hierarchy["PRUID"] = int(pr_id)
            hierarchy["PRNAME"] = self.prid_to_prname.get(str(pr_id))

        return hierarchy
