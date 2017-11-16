"""
Support for broadcasting valuesets and using them in user-defined functions
in Spark queries.
"""

from collections import namedtuple

from bunsen.mapping import get_default

# Placeholder record to load descendents of a given ancestor
AncestorPlaceholder = namedtuple("AncestorPlaceholder",
                                 "codeSystem codeValue conceptMapUri conceptMapVersion")

def isa_loinc(code_value, loinc_version=None):
    """
    Returns a valueset placeholder that will load all values that are descendents
    of a given LOINC code.
    """
    return AncestorPlaceholder('http://loinc.org',
                               code_value,
                               'uri:cerner:foresight:mapping:loinc-hierarchy',
                               loinc_version)

def isa_snomed(code_value, snomed_version=None):
    """
    Returns a valueset placeholder that will load all values that are descendents
    of a given SNOMED code.
    """
    return AncestorPlaceholder('http://snomed.info/sct',
                               code_value,
                               'uri:cerner:foresight:mapping:snomed-hierarchy',
                               snomed_version)


def push_valuesets(spark_session, valueset_map, concept_maps=None):
    """
    Pushes valuesets onto a stack and registers an in_valueset user-defined function
    that uses this content.

    The valueset_map takes the form of {referenceName: [(codeset, codevalue), (codeset, codevalue)]}
    to specify which codesets/values are used for the given valueset reference name.

    Rather than explicitly passing a list of (codeset, codevalue) tuples, users may instead provide
    an AncestorPlaceholder that instructs the the system to load all descendents of a given
    code value. See the isa_loinc and isa_snomed functions above for details.
    """
    if concept_maps is None:
        concept_maps = get_default(spark_session)

    jvm = spark_session._jvm

    builder = jvm.com.cerner.bunsen.mappings.broadcast.BroadcastableValueSets.newBuilder()

    for (name, content) in valueset_map.items():

        if type(content) is AncestorPlaceholder:

            # Add descendents of the specified item
            (codeSystem, codeValue, conceptMapUri, conceptMapVersion) = content

            builder.addDescendantsOf(name, codeSystem, codeValue, conceptMapUri, conceptMapVersion)

        else:

            # Add the explicitly provided code values
            for (codeSystem, codeValue) in content:
                builder.addCode(name, codeSystem, codeValue)

    broadcastable = builder.build(spark_session._jsparkSession, concept_maps._jconcept_maps)

    jvm.com.cerner.bunsen.ValueSetUdfs.pushUdf(spark_session._jsparkSession, broadcastable)

def get_current_valuesets(spark_session):
    """
    Returns the current valuesets in the same form that is accepted by
    the push_valuesets function above, that is the structure will follow this pattern:
    {referenceName: [(codeset, codevalue), (codeset, codevalue)]}
    """
    jvm = spark_session._jvm

    current = jvm.com.cerner.bunsen.ValueSetUdfs.currentValueSets()

    if current is None:
        return None
    else:
        valuesets = current.getValue()

        return {name: [(system, value)
                       for system in valuesets.getValues(name).keySet()
                       for value in valuesets.getValues(name).get(system)]
                for name in valuesets.getReferenceNames()}

def pop_valuesets(spark_session):
    """
    Pops the current valuesets from the stack, returning true if there remains
    an active valueset, or false otherwise.
    """
    jvm = spark_session._jvm

    return jvm.com.cerner.bunsen.ValueSetUdfs.popUdf(spark_session._jsparkSession)