import os
import ast
import re
from pyflowchart import Flowchart


class FunctionVisitor(ast.NodeVisitor):
    def __init__(self):
        self.functions = []

    def visit_FunctionDef(self, node):
        function_name = f"{node.parent.name}.{node.name}" if isinstance(node.parent, ast.ClassDef) else node.name
        self.functions.append(function_name)
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        for child in node.body:
            child.parent = node  # Set the parent for each function in the class
        self.generic_visit(node)


def get_functions_from_code(code):
    """Parse the code and return a list of functions."""
    tree = ast.parse(code)
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node  # Set parent for each node
    visitor = FunctionVisitor()
    visitor.visit(tree)
    return visitor.functions


def remove_triple_quoted_strings(code):
    """Remove triple-quoted strings from the code."""
    pattern = r'""".*?"""|\'\'\'.*?\'\'\''
    return re.sub(pattern, '', code, flags=re.DOTALL)


def generate_flowchart(code, function_name):
    """Generate a flowchart for a given function in the code."""
    try:
        flowchart = Flowchart.from_code(code=code, field=function_name, inner=True)
        return f"## Flowchart for {function_name}\n{flowchart.flowchart()}\n"
    except Exception as e:
        print(f"Error processing function {function_name}: {str(e)}")
        return None


def process_file(file_path, exclude_patterns):
    """Process a file and return flowcharts for each function."""
    if any(pattern in file_path for pattern in exclude_patterns):
        return None

    with open(file_path, 'r') as f:
        code = remove_triple_quoted_strings(f.read())

    functions = get_functions_from_code(code)
    return '\n'.join(filter(None, (generate_flowchart(code, fn) for fn in functions)))


def process_directory(directory, exclude_patterns):
    """Process all Python files in a directory and return flowcharts."""
    all_flowcharts = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                file_flowcharts = process_file(file_path, exclude_patterns)
                if file_flowcharts:
                    all_flowcharts.append(file_flowcharts)
    return '\n'.join(all_flowcharts)


def save_to_file(content, filename):
    """Save content to a file."""
    with open(filename, 'w') as f:
        f.write(content)


if __name__ == "__main__":
    project_directory = '.'  # Change if needed
    exclude_patterns = ['test', 'venv', '.git', '__pycache__']
    
    # Generate flowcharts
    result = process_directory(project_directory, exclude_patterns)
    
    # Print or save the result
    print(result)
    save_to_file(result, 'flowchart.txt')