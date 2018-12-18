# Author: cxyang
'''
Input:
string id, id, id, id

Output:
list[a1, a2, ...]
a1: {
    'title':,
    'text':,
    'value':
}
'''
import map_offset
import xml.dom.minidom

FILE_PATH = "../../data/enwikisource.xml"
f = open(FILE_PATH, 'r')


def extract_xml(offset, length):

    f.seek(offset)
    page = f.read(length)
    # print(page)

    dom = xml.dom.minidom.parseString(page)
    root = dom.documentElement
    title = root.getElementsByTagName('title')[0].firstChild.data
    text = root.getElementsByTagName('text')[0].firstChild.data

    return title, text


def get_xml_content(string_id):
    res = []
    list_res_map = map_offset.sol(string_id)
    # print(list_res_map)
    
    for each_dict in list_res_map:
        this_res = {
            "title": "",
            "text": "",
            "id": "",
        }
        offset = each_dict['offset']
        length = each_dict['length']
        title, text = extract_xml(offset - length, length)

        this_res['title'] = title
        this_res['text'] = text
        this_res['id'] = each_dict['id']

        res.append(this_res)
        
    return res


# if __name__ == "__main__":
#     res = get_xml_content("1,2")
#     for i in res:
#         print(i)

