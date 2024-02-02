from jinja2 import Template
from compare_datasets.structure import stringify_result
template_string = """
<section id="{{ name }}">
<h2>{{ name }}</h2> 

<p class="sidenote">{{ explanation }}</p>

<h5 class={{stringified_result}}>{{ stringified_result }}</h5>

<p>{{ html_report }}</p>

<p>{{ conclusion }}</p>
</section>
"""

def generate_html_element (report_dictionary):    
    template = Template(template_string)
    report_dictionary['stringified_result'] = stringify_result(report_dictionary['result'])
    if isinstance(report_dictionary, list):
        print(report_dictionary[-1])
        for r in report_dictionary:
            print(r['name'])
            print(r['conclusion']) 
        output = [template.render(report) for report in report_dictionary]
    else:
        output = template.render(report_dictionary)
    return output

def generate_body(compare_object):
    html_elements = []
    for attribute in ['data','jaccard_similarity', 'string_comparisons', 'numeric_comparisons', 'datetime_comparisons', 'boolean_comparisons']:
        if hasattr(compare_object, attribute):
            element = generate_html_element(getattr(compare_object, attribute).report)
            if isinstance(element, list):
                html_elements.extend(element)
            else:
                html_elements.append(element)
    return "\n".join(html_elements)