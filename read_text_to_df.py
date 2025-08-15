import pandas as pd

def read_text_to_dataframe(file_path):
    # Read all lines from the text file
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Remove any trailing newlines and create DataFrame
    lines = [line.strip() for line in lines]
    df = pd.DataFrame(lines, columns=['text'])
    return df

# Example usage
if __name__ == "__main__":
    # Create a sample text file
    sample_text = """Hello
    This is line 2
    And this is line 3
    Each line will be a cell"""
    
    # Write sample text to a file
    with open('sample.txt', 'w') as f:
        f.write(sample_text)
    
    # Read the file into a DataFrame
    df = read_text_to_dataframe('sample.txt')
    
    # Display the result
    print("\nDataFrame contents:")
    print(df) 