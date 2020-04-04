import pandas as pd
import numpy as np
import random
from sklearn.model_selection import ParameterGrid
import multiprocessing
import os

def calculate_error(dia, thresh_up, thresh_bot):
    filtered = df.loc[df['dia'] == dia]
    filtered = filtered.assign(prob_thresh=np.nan,
                               provisionado_real = df['valor_montante']*df['prob_real'],
                               provisionado_thresh = np.nan)
    filtered['prob_thresh'] = np.where(filtered['prob_predita'] >= thresh_up,
                                               1,
                                               filtered['prob_predita'])
    filtered['prob_thresh'] = np.where(filtered['prob_predita'] <= thresh_bot,
                                               0,
                                               filtered['prob_predita'])
    filtered = filtered.assign(provisionado_thresh = filtered['prob_thresh'] * filtered['valor_montante'])
    provisionado_real = np.sum(filtered.provisionado_real.values)
    provisionado_predito = np.sum(filtered.provisionado_thresh.values)
    erro = np.abs(provisionado_real - provisionado_predito) / provisionado_real
    return(erro)

def calculate_parallel(dict):
    erro = calculate_error(**dict)
    return(erro)

if __name__ == "__main__":

    prob_predita = np.random.rand(10000000)
    prob_real = np.zeros(6000000).tolist()
    prob_real.extend(np.ones(4000000).tolist())
    random.shuffle(prob_real)
    dia = np.array([1,2,3,4,5])
    dia = np.repeat(dia,2000000)
    random.shuffle(dia)
    valor = np.random.randint(low=1, high=1000, size=10000000)


    df = pd.DataFrame({'dia':dia,
                    'prob_predita':prob_predita,
                    'prob_real':prob_real,
                    'valor_montante':valor})

    grid = [{'dia':df.dia.unique(),
            'thresh_up':np.arange(0.5,1.1,0.1),
            'thresh_bot':np.arange(0,0.6,0.1)}]

	
    lista_iteravel = list(ParameterGrid(grid))
    pool = multiprocessing.Pool(4)
    resultados = pool.map(calculate_parallel,lista_iteravel)

    #resultados = list(map(lambda x: calculate_error(**x),list(ParameterGrid(grid))))


Ideias de melhora PDD

Primeiro de tudo avaliar o resultado e depois verificar necessidade de melhorar a modelagem. Construir variáveis olhando para o passado pode ser muito bom.
Mas pode dar trabalho excessivo num momento que estamos sem tempo.

1 - Criar uma variável binária para indicar se houve corte ou não dentro do mês. Como fazer isso se o CORTE_5, que é o mais próximo que temos do corte pontual dentro do mês,
some depois de 5 dias ? As 4 variáveis de corte já não deveriam pegar esse efeito ?
-> Trazer a data do corte nas ações de cobrança
-> Agrupar os arquivos e verificar existência de corte para aquele documento (viável para entrantes, inviável para saintes)

2 -  Contabilizar variação de algumas variáveis em relação ao registro anterior (QTD_ACOES, QTD_ACOES_ANTERIOR, DELTA_DFDMES (captura a quantidade de dias entre variações nas ações de cobrança). Como fazer isso 
sem todos os registros do mes empilhados ?

Empilhar registros de to_predict só é possível para entrantes por causa da memória consumida. Se empilhados é só pegar a prob predita em max DFDMES e usar um PROCV de documento probabilidade predita.

3 - Usar time lag nas variáveis.
Problema: Os arqujivos para predição diária não contemplam os estados anteriores dos documentos no mesmo mês. Para viabilizar teríamos de agregar os arquivos e contabilizar as variações.

4. Usar Log(Atraso).

