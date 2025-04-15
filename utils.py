import surt


def thing_to_surt_host_name(thing):
    '''convert a thing, probably an url of some kind, to a surt host name'''
    orig = thing
    if '/' not in thing and '.' not in thing:
        if thing.endswith(',*'):
            thing = thing[:-1]
        if '*' in thing:
            raise ValueError('unexpected * in '+thing)
        if ',,' in thing:
            raise ValueError('unexpected ,, in '+thing)
        return thing
    surt_host_name, extra = surt.surt(thing).split(')/', 1)
    if extra:
        raise ValueError(f'skipping {orig} because extra is {extra}')
    if surt_host_name.endswith(',*'):
        surt_host_name = surt_host_name[:-1]
    if ',,' in  surt_host_name:
        raise ValueError('unexpected ,, in '+thing)
    return surt_host_name
