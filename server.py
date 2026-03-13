import socket
import threading
import time
import logging

HOST = "0.0.0.0"      
PORT = 9000           
NODE_TIMEOUT = 10    
CLEAN_INTERVAL = 5  

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server")

nodes: dict[str, float] = {}
nodes_lock = threading.Lock()


def cleaner() -> None:
    while True:
        time.sleep(CLEAN_INTERVAL)
        now = time.time()
        with nodes_lock:
            expired = [nid for nid, ts in nodes.items()
                       if now - ts > NODE_TIMEOUT]
            for nid in expired:
                del nodes[nid]
                log.info("Nó expirado removido automaticamente: %s", nid)



def process_message(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return "ERROR:EMPTY_COMMAND"

    if raw.startswith("REGISTER:"):
        node_id = raw[len("REGISTER:"):]
        if not node_id:
            return "ERROR:MISSING_NODE_ID"
        with nodes_lock:
            if node_id in nodes:
                return "ERROR:ALREADY_REGISTERED"
            nodes[node_id] = time.time()
        log.info("Nó registrado: %s", node_id)
        return "OK:REGISTERED"

    elif raw.startswith("HEARTBEAT:"):
        node_id = raw[len("HEARTBEAT:"):]
        if not node_id:
            return "ERROR:MISSING_NODE_ID"
        with nodes_lock:
            if node_id not in nodes:
                return "ERROR:NOT_REGISTERED"
            nodes[node_id] = time.time()
        log.debug("Heartbeat recebido: %s", node_id)
        return "OK:HEARTBEAT"

    elif raw == "LIST":
        now = time.time()
        with nodes_lock:
            active = [nid for nid, ts in nodes.items()
                      if now - ts <= NODE_TIMEOUT]
        log.info("LIST → %d nó(s) ativo(s): %s", len(active), active)
        return "NODES:" + ",".join(sorted(active))

    elif raw.startswith("QUIT:"):
        node_id = raw[len("QUIT:"):]
        with nodes_lock:
            nodes.pop(node_id, None)
        log.info("Nó desconectado via QUIT: %s", node_id)
        return "OK:BYE"

    else:
        log.warning("Comando desconhecido recebido: %r", raw)
        return "ERROR:UNKNOWN_COMMAND"



def handle_client(conn: socket.socket, addr: tuple) -> None:
    log.info("Nova conexão: %s:%d", *addr)
    registered_node: str | None = None   

    try:
        with conn.makefile("r", encoding="utf-8", newline="\n") as reader:
            for line in reader:
                stripped = line.strip()
                if stripped.startswith("REGISTER:"):
                    candidate = stripped[len("REGISTER:"):]
                    if candidate:
                        registered_node = candidate

                response = process_message(line)
                conn.sendall((response + "\n").encode("utf-8"))

                if response == "OK:BYE":
                    registered_node = None 
                    break

    except (ConnectionResetError, BrokenPipeError, OSError) as exc:
        log.warning("Conexão encerrada abruptamente (%s:%d): %s", *addr, exc)
    except Exception as exc:
        log.error("Erro inesperado no cliente %s:%d: %s", *addr, exc)
    finally:
        if registered_node is not None:
            with nodes_lock:
                nodes.pop(registered_node, None)
            log.info("Nó removido por queda de conexão: %s", registered_node)
        conn.close()
        log.info("Conexão encerrada: %s:%d", *addr)


def main() -> None:
    t_clean = threading.Thread(target=cleaner, daemon=True, name="cleaner")
    t_clean.start()
    log.info("Thread cleaner iniciada (intervalo: %ds, timeout: %ds)",
             CLEAN_INTERVAL, NODE_TIMEOUT)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((HOST, PORT))
        server_sock.listen()
        log.info("Servidor escutando em %s:%d — aguardando conexões...", HOST, PORT)

        try:
            while True:
                conn, addr = server_sock.accept()
                t = threading.Thread(
                    target=handle_client,
                    args=(conn, addr),
                    daemon=True,
                    name=f"client-{addr[0]}:{addr[1]}",
                )
                t.start()
        except KeyboardInterrupt:
            log.info("Servidor encerrado pelo usuário (Ctrl+C).")


if __name__ == "__main__":
    main()