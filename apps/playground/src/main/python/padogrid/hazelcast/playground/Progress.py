import uuid

class Progress():
    def __init__(self, ds_type, ds_name, key_type, key_range, object_type, er_object_name, delta, operation, max, batch_size, delay_in_msec, callback=None):
        self.ds_type = ds_type
        self.ds_name = ds_name
        self.key_type = key_type
        self.key_range = key_range
        self.object_type = object_type
        self.er_object_name = er_object_name
        self.delta = delta
        self.operation = operation
        self.max = max
        self.batch_size = batch_size
        self.delay_in_msec = delay_in_msec
        self.count = 0
        self.callback = callback
        self.job_id = str(uuid.uuid4())

    def __str__(self):
        if self.key_type == None:
            key_type = "N/A"
        else:
            key_type = self.key_type
        if self.er_object_name == None:
            er_object_name = "N/A"
        else:
            er_object_name = self.er_object_name
        return self.ds_type + "." + self.ds_name + ": " \
               + self.operation  \
               + " key(" + key_type + ")" \
               + " key_range(" + str(self.key_range) + ")" \
               + " object(" + self.object_type + ")" \
               + " er()" + er_object_name + ")" \
               + " delta(" + str(self.delta) + ")"  \
               + " batch_size(" + str(self.batch_size) + ")"  \
               + " delay(" + str(self.delay_in_msec) + " ms) ("  \
               + str(self.count) + "/" + str(self.max) + ")"