
def flatten_to_array(d, parent_key='', sep='_'):
    items = []
    new_d = None
    if type(d) == list:
        new_d = {}
        for i, v in enumerate(d):
            new_d[str(i)] = v
    elif type(d) == int:
        return {"0":d}
    elif type(d) == unicode:
        return {"0":d}
    elif type(d) == float:
        return {"0":d}
    else: 
        new_d = d

    key_in_array = ""
    if "key" in new_d:
        key_in_array = "_" + str(new_d["key"])

    for k, v in new_d.items():
        if k == "key":
            continue
        new_key = (parent_key + sep + k if parent_key else k) + key_in_array
        if isinstance(v, collections.MutableMapping) or type(v) == list:
            items.extend(flatten_to_array(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
