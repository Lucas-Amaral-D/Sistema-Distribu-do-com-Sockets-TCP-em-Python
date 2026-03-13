import socket
import sys
import time
import threading
import logging

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9000
HEARTBEAT_INTERVAL = 3  
DEMO_DURATION = 30      

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


class NodeClient:

    def __init__(self, node_id: str, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.node_id = node_id
        self.host = host
        self.port = port
        self._sock: socket.socket | None = None
        self._reader = None
        self._sock_lock = threading.Lock()   
        self.log = logging.getLogger(node_id)


    def connect(self) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        self._reader = self._sock.makefile("r", encoding="utf-8", newline="\n")
        self.log.info("Conectado a %s:%d", self.host, self.port)

    def close(self) -> None:
        with self._sock_lock:
            if self._sock:
                self._sock.close()
                self._sock = None
                self._reader = None

    def _send(self, message: str) -> str:
        with self._sock_lock:
            if not self._sock:
                raise RuntimeError("Cliente não está conectado ao servidor.")
            self._sock.sendall((message + "\n").encode("utf-8"))
            response = self._reader.readline().strip()
        return response

    def register(self) -> str:
        resp = self._send(f"REGISTER:{self.node_id}")
        self.log.info("REGISTER → %s", resp)
        return resp

    def heartbeat(self) -> str:
        resp = self._send(f"HEARTBEAT:{self.node_id}")
        self.log.debug("HEARTBEAT → %s", resp)
        return resp

    def list_nodes(self) -> list[str]:
        resp = self._send("LIST")
        self.log.info("LIST → %s", resp)
        if resp.startswith("NODES:"):
            raw = resp[len("NODES:"):]
            return [n for n in raw.split(",") if n]
        return []

    def quit(self) -> str:
        resp = self._send(f"QUIT:{self.node_id}")
        self.log.info("QUIT → %s", resp)
        self.close()
        return resp

def run_demo(node_id: str, host: str, port: int) -> None:
    client = NodeClient(node_id, host, port)
    try:
        client.connect()
    except ConnectionRefusedError:
        print(f"Erro: não foi possível conectar a {host}:{port}. O servidor está rodando?")
        return

    resp = client.register()
    print(f"Registro: {resp}")
    if "ERROR" in resp:
        client.close()
        return

    stop_event = threading.Event()

    def heartbeat_loop() -> None:
        while not stop_event.wait(HEARTBEAT_INTERVAL):
            try:
                r = client.heartbeat()
                if "ERROR" in r:
                    print(f"Heartbeat recusado: {r}")
            except Exception as exc:
                print(f"Erro ao enviar heartbeat: {exc}")
                stop_event.set()
                break

    t = threading.Thread(target=heartbeat_loop, daemon=True, name="heartbeat")
    t.start()

    elapsed = 0
    while elapsed < DEMO_DURATION and not stop_event.is_set():
        time.sleep(5)
        elapsed += 5
        try:
            active = client.list_nodes()
            print(f"[{elapsed:>3}s] Nós ativos ({len(active)}): {active}")
        except Exception as exc:
            print(f"Erro ao listar nós: {exc}")

    stop_event.set()
    t.join(timeout=HEARTBEAT_INTERVAL + 1)

    try:
        print(f"Encerrando: {client.quit()}")
    except Exception:
        client.close()

    print("Demo concluído.")


MENU = """
╔══════════════════════════════════════╗
║   Sistema Distribuído — Cliente      ║
╠══════════════════════════════════════╣
║  1. Registrar nó                     ║
║  2. Enviar heartbeat                 ║
║  3. Listar nós ativos                ║
║  4. Desconectar (envia QUIT)         ║
║  0. Sair sem avisar o servidor       ║
╚══════════════════════════════════════╝"""


def run_interactive(node_id: str, host: str, port: int) -> None:
    client = NodeClient(node_id, host, port)
    try:
        client.connect()
    except ConnectionRefusedError:
        print(f"Erro: não foi possível conectar a {host}:{port}. O servidor está rodando?")
        return

    print(MENU)
    while True:
        try:
            choice = input("\nEscolha uma opção: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nInterrompido. Encerrando sem avisar o servidor.")
            client.close()
            break

        if choice == "1":
            print(f"  → {client.register()}")

        elif choice == "2":
            print(f"  → {client.heartbeat()}")

        elif choice == "3":
            active = client.list_nodes()
            if active:
                print(f"  Nós ativos ({len(active)}): {', '.join(active)}")
            else:
                print("  Nenhum nó ativo no momento.")

        elif choice == "4":
            print(f"  → {client.quit()}")
            break

        elif choice == "0":
            client.close()
            print("  Conexão encerrada localmente (sem QUIT).")
            break

        else:
            print("  Opção inválida. Escolha entre 0 e 4.")

def main() -> None:
    args = sys.argv[1:]
    demo_mode = "--demo" in args
    if demo_mode:
        args.remove("--demo")

    node_id = args[0] if len(args) > 0 else (input("ID do nó: ").strip() or "node1")
    host    = args[1] if len(args) > 1 else DEFAULT_HOST
    port    = int(args[2]) if len(args) > 2 else DEFAULT_PORT

    print(f"Nó: {node_id!r}  |  Servidor: {host}:{port}"
          + ("  |  Modo: DEMO" if demo_mode else "  |  Modo: interativo"))

    if demo_mode:
        run_demo(node_id, host, port)
    else:
        run_interactive(node_id, host, port)


if __name__ == "__main__":
    main()