# PUC Data Science
***Projeto MVP de Engenharia de Dados***
***Matéria: Engenharia de Dados***
***Databricks***

![Daabricks](https://th.bing.com/th/id/OIP._tSNl4WZRbmfPgDqRaodSwHaD6?rs=1&pid=ImgDetMain)


Este projeto representa o MVP desenvolvido na pós-graduação em Data Science para a disciplina de Engenharia de Dados. O objetivo principal é **identificar os fatores comportamentais e socioeconômicos que influenciam negativamente o desempenho acadêmico dos alunos**.

## Sumário

- [Visão Geral](#visao-geral)
- [Objetivo](#objetivo)
- [Metodologia](#metodologia)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Resultados e Analises](#resultados-e-An%C3%A1lises)
- [Conclusao](#Conclus%C3%A3o)

## Visão Geral

Neste projeto, integrei dados acadêmicos, socioeconômicos e comportamentais para analisar o desempenho dos alunos. Utilizando a plataforma **Databricks Community Edition**, o **Apache Spark (PySpark)** e o **Delta Lake**, criei um fluxo que percorre as seguintes camadas:

- **BRONZE**: Armazenamento dos dados brutos extraídos do arquivo CSV.
- **PRATA**: Transformação dos dados para a criação das dimensões e da tabela fato.
- **OURO**: Integração dos dados em uma tabela unificada para análises detalhadas.

## Objetivo 🎯

O projeto visa identificar e quantificar os fatores que prejudicam o desempenho acadêmico. Com essa análise, procuro evidenciar que aspectos como **alta concentração de estresse**, **horas de sono reduzidas**, **baixa escolaridade dos pais**, e **acesso limitado à internet** se correlacionam fortemente com o conceito final dos alunos.

## Metodologia 🔍

A modelagem foi estruturada com base em um fluxo de dados em três camadas:

- **BRONZE**: Armazenei os dados originais obtidos do arquivo CSV, mantendo a integridade dos dados brutos.
- **PRATA**: Efetuei transformações para criar:
  - **Dimensão ALUNOS**: contendo informações pessoais e socioeconômicas, como ID, nome, gênero, idade, escolaridade dos pais e renda familiar.
  - **Dimensão DISCIPLINAS**: com os departamentos únicos, gerando um ID incremental para cada disciplina.
  - **Dimensão HÁBITOS**: dados sobre estudo, atividades extracurriculares, acesso à internet, estresse e sono.
  - **Fato DESEMPENHO**: que agrega as avaliações dos alunos, como presença, notas e conceito final.
- **OURO**: Integrei os dados das dimensões em uma tabela unificada, criando a view `alunos_com_desempenho`, que categoriza os alunos em grupos de desempenho (ex.: “Alto”, “Medio” e “Baixo”).

## Tecnologias Utilizadas ⚙️

- **Databricks Community Edition**
- **Apache Spark (PySpark)**
- **Delta Lake**
- **SQL e Python**  
  - **Pandas, Matplotlib e Plotly** para análise e visualizações

## Resultados e Análises 📊

A partir da análise dos dados, foram identificados os seguintes pontos:

- **Horas de Estudo**: A média de horas estudadas é praticamente a mesma entre os grupos, indicando que o tempo de estudo não é um diferencial significativo.
- **Horas de Sono**: Alunos com desempenho mais baixo dormem, em média, menos horas por noite (por exemplo, o grupo de conceito F registra cerca de 6,1h, em comparação com 7,1h do grupo A/B).
- **Estresse**: Alunos com desempenho ruim apresentam uma proporção muito maior de níveis de estresse elevados (≥ 7). Por exemplo, no grupo com conceito F, ~43,4% dos alunos possuem estresse alto contra ~16,9% no grupo de alto desempenho.
- **Escolaridade dos Pais**: A baixa escolaridade é prevalente em 55,2% dos alunos do grupo F, enquanto apenas 26,8% dos alunos de alto desempenho apresentam essa condição, mostrando a influência do ambiente familiar.
- **Atividades Extracurriculares**: Apenas 29,1% dos alunos com conceito F participam de atividades extracurriculares, contrastando com 49,2% entre os alunos de melhor desempenho.
- **Acesso à Internet**: A falta de acesso à internet em casa é mais marcada entre os alunos de baixo desempenho, com 16,3% sem acesso, comparado a 4,1% no grupo de alto desempenho.

## Conclusão 💡

Os dados indicam que o desempenho acadêmico não depende apenas do esforço individual, mas está fortemente correlacionado com fatores emocionais e contextuais. Alunos que enfrentam altos níveis de estresse, dormem menos, têm acesso limitado à internet, participam menos de atividades extracurriculares e vêm de um contexto familiar com menor escolaridade apresentam resultados significativamente inferiores.

Esses insights sugerem que, para melhorar o desempenho, é necessário:

- **Investir em programas de suporte psicológico e de gerenciamento de estresse.**
- **Implementar ações que promovam o engajamento e suporte familiar**, especialmente para famílias com menor escolaridade.
- **Ampliar o acesso digital** para reduzir desigualdades de oportunidade.
- **Estimular a participação em atividades extracurriculares**, que favorecem o desenvolvimento socioemocional.

A combinação dessas estratégias pode proporcionar uma abordagem mais integrada e eficaz na melhoria do desempenho escolar, permitindo que a educação atenda de maneira mais justa e personalizada às necessidades dos alunos.
