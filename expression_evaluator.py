from pyeda.inter import *


def parse_boolean_expression(boolean_exp,num_variables):
	#TODO: Find a better way to convert text variables to boolean variables
	count = 0
	boolean_exp_list = list(boolean_exp)
	for index,char in enumerate(boolean_exp_list):
		if char.isalpha():
			boolean_exp_list[index] = "Z[{}]".format(count)
			count += 1
	final_bool_exp = "".join(boolean_exp_list)

	Z = exprvars("z", num_variables)
	expression = final_bool_exp
	return expression.to_dnf()
