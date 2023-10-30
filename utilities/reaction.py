

from submodules.utilities.util import get_number_from_string, get_str_from_string


def convert_partial_reactionstr_to_inl(reaction):
    ## convert the  partial reaction string format to exfor code
    if reaction.split(',')[0].upper() == reaction.split(',')[1][0].upper():
        ## (n,n1) --> (N,INL) but if (n,p1) then next
        ## (a,a1) --> (A,INL)
        return f"{reaction.split(',')[0].upper()},INL"
    else:
        ## such as (n,a1) --> (N,A)
        return get_str_from_string(reaction).upper()


def convert_reaction_to_exfor_style(reaction):
    if reaction.split(',')[0].upper() == "H":
        ## incident "h" --> "HE3"
        reaction = f"HE3,{reaction.split(',')[1].upper()}"
    
    return reaction

