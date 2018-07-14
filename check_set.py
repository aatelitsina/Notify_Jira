class CS (object):
    def __init__(self,row):
        self.src_code = row[1]
        self.pkg_id = row[2]
        self.tgt_layer = row[3]
        self.period = row[4]