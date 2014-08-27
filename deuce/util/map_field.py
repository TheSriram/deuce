def map_field(type, from_dict, to_dict, field_name, header_name):

    try:
        to_dict[field_name] = type(from_dict[header_name])
    except (KeyError, ValueError):
        to_dict[field_name] = type()
