import numpy as np
import argparse

def calcular_min_max(lista_numeros, verbose=1):
    '''
    Retorna los valores minimo y maximo de una de lista de numeros
    Args:
        lista_numeros: type list
    '''
    min_value = np.min(lista_numeros)
    max_value = np.max(lista_numeros)

    if verbose == 1:
        print('Valor minimo:', min_value)
        print('Valor maximo:', max_value)
    else:
        pass
    return min_value, max_value

def calcular_valores_centrales(lista_numeros, verbose=1):
    """Calcula la media y las desviacion estandar de una lista de
    numeros
    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): para decidir si imprimir mensajes en pantalla. Defaults to True.
    Returns:
        tuple: (media, dev_std)
    """
    media   = np.mean(lista_numeros)
    dev_std = np.std(lista_numeros)

    if verbose == 1:
        print('Media:', media)
        print('Desviacion Estandar:', dev_std)
    else:
        pass
    return media, dev_std

def calcular_valores(lista_numeros, verbose=1):
    """Retorna una tupla con valores suma, minimo, maximo
    media y desviacion estandar de un lista de numeros
    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): para decidir si imprimir mensajes en pantalla. Defaults to True.
    Returns:
        _type_: _description_
    """
    suma             = np.sum(lista_numeros)   # calcular_suma(lista_numeros)
    min_val, max_val = calcular_min_max(lista_numeros, verbose)
    media, dev_std   = calcular_valores_centrales(lista_numeros, verbose)

    return suma, min_val, max_val, media, dev_std


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--verbose", 
        type=int, 
        default=1, 
        help="para imprimir en pantalla informacion"
    )
    args = parser.parse_args()

    lista_valores = [5, 4, 8, 9, 21]   
    calcular_valores(lista_numeros = lista_valores, verbose = args.verbose)

if __name__ == '__main__':
    main()