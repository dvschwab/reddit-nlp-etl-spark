

def str_to_dict(str):

    res = []
    for element in str.split(', '):
        if ':' in element:
            res.append(map(str.strip, element.split(':', 1)))
    res_dict = dict(res)

    return (res_dict)