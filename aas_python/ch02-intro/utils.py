def makeMatchData(data):
    obj = MatchData(data)
    return obj
# end def

class MatchData:
    def __init__(self, data):
        id_1, id_2, cmp_fname_c1, cmp_fname_c2, \
        cmp_lname_c1, cmp_lname_c2, cmp_sex, \
        cmp_bd, cmp_bm, cmp_by, cmp_plz, is_match = data
        
        self.id_1 = int(float(id_1))
        self.id_2 = int(float(id_2))
        self.cmp_fname_c1 = float(cmp_fname_c1) if cmp_fname_c1 is not None else 0
        self.cmp_fname_c2 = float(cmp_fname_c2) if cmp_fname_c2 is not None else 0
        self.cmp_lname_c1 = float(cmp_lname_c1) if cmp_lname_c1 is not None else 0
        self.cmp_lname_c2 = float(cmp_lname_c2) if cmp_lname_c2 is not None else 0
        self.cmp_sex  = int(float(cmp_sex)) if cmp_sex is not None else 0
        self.cmp_bd   = int(float(cmp_bd))  if cmp_bd  is not None else 0
        self.cmp_bm   = int(float(cmp_bm))  if cmp_bm  is not None else 0
        self.cmp_by   = int(float(cmp_by))  if cmp_by  is not None else 0
        self.cmp_plz  = int(float(cmp_plz)) if cmp_plz is not None else 0
        self.is_match = is_match
    # end def
# end class
