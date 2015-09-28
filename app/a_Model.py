def ModelIt(fromUser = 'Default', population = 0):
    print "The population is {0}".format(population)
    result = population/1000000.0
    if fromUser != 'Default':
        return result
    else:
        return "check your input"