# Author: cxyang
'''
Input: 
word
Output:
'''
import get_xml_content

FILE_PATH = "../../data/prerank.txt"

def search():
    while(True):
        word = raw_input('--------------------Enter the Keyword--------------------\n>')
        if word == "EXIT":
            break
        
        f = open(FILE_PATH, 'r')
        
        string_id = ""
        flag = 0 
        for line in f:
            cur_word = line.split()[0]
            if(word == cur_word):
                string_id = line.split()[1]
                flag = 1

        if flag == 0:
            print("--------------------Your search did not match any documents--------------------")
            continue
        
        res = get_xml_content.get_xml_content(string_id)

        print("--------------------Here's the result--------------------")
        
        for each_dict in res:
            print(each_dict['id'] + '-----' + each_dict['title'])
        
        enter_id = raw_input('Which one do you like? ')
        
        for each_dict in res:
            if enter_id == each_dict['id']:
                print("--------------------Here's the content--------------------")
                print(each_dict['text'])

    return


if __name__ == "__main__":
    search()