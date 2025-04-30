#Para conectar ao EDC (Catalogo de dados)
import getpass
import base64

def autenticacao():
    username = input("Digite o seu Username de rede: ")
    senhaDeRede = getpass.getpass(prompt="Senha do Usuario T= " + username + ": ")
    auth = f"{username}:{senhaDeRede}"
    base64_ = base64.b64encode(bytes(auth,"utf-8"))
    return base64_.decode("utf-8")
