#Removes special characters
def remove_special_characters(input_string):
    result = ''.join(char for char in input_string if char.isalnum())
    return result


    

