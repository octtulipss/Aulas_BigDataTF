# Constrói a imagem (certifique-se de que codigo spark e Dockerfile estão na pasta)
docker build -t meu-ambiente-pyspark .

# Mapeia a porta (-p 8888:8888) e monta um volume para persistência dos dados
docker run -it --rm -p 8888:8888 -v ~/folder_data:/home/projetos/pyspark/data meu-ambiente-pyspark