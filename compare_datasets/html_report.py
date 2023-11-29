from jinja2 import Template

template_string = """
<section id="{{ name }}">
<h2>{{ name }}</h2>

<p>{{ explanation }}</p>

<h5 data-status={{result}}>{{ result }}</h5>

<p>{{ html_report }}</p>

<p>{{ conclusion }}</p>
</section>
"""

def generate_html_element (report_dictionary):
    template = Template(template_string)
    output = template.render(report_dictionary)
    return output

def generate_body(compare_object):
    html_elements = []
    for attribute in ['jaccard_similarity', 'string_comparisons', 'numeric_comparisons', 'datetime_comparisons', 'boolean_comparisons']:
        if hasattr(compare_object, attribute):
            html_elements.append(generate_html_element(getattr(compare_object, attribute).report))
    return "\n".join(html_elements)