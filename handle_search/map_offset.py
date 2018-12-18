# Author: cxyang
'''
Input: 
string id, id, id, id

Output: 
List[
    {'offset': , 'length': }...
]
'''

FILE_PATH = "../../data/position.txt"


def split_input(string_id):
    list_id = string_id.split(',')
    return list_id


def search_file(each_id):
    f = open(FILE_PATH, 'r')

    for line in f:
        cur_id = line.split()[0]
        if(each_id == cur_id):
            offset = int(line.split()[1])
            length = int(line.split()[2])

    return offset, length


def get_res(each_id):
    one_res = {
        "id": "",
        "offset": 0,
        "length": 0,
    }

    offset, length = search_file(each_id)

    one_res['id'] = each_id
    one_res['offset'] = offset
    one_res['length'] = length

    return one_res


def sol(string_id):
    list_res = []
    list_id = split_input(string_id)

    for each_id in list_id:
        one_res = get_res(each_id)
        list_res.append(one_res)
    
    return list_res


# if __name__ == "__main__":
#     print(sol('1,2'))