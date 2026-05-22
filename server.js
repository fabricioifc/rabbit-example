const express = require('express');
const amqp = require('amqplib');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const QUEUE_NAME = 'fila_relatorios';
const DLX_EXCHANGE = 'dlx_exchange';
const DLX_QUEUE = 'fila_erros_relatorios';
const RABBITMQ_URL = 'amqp://rabbitmq';
const FILE_PATH = path.join(__dirname, 'historico.json');

let connection = null;
let channel = null;

// Função para ler o histórico do arquivo JSON de forma segura
function lerHistorico() {
    try {
        if (!fs.existsSync(FILE_PATH)) return [];
        const dados = fs.readFileSync(FILE_PATH, 'utf-8');
        return dados ? JSON.parse(dados) : [];
    } catch (e) {
        return [];
    }
}

// Função para salvar o histórico no arquivo JSON
function salvarHistorico(historico) {
    fs.writeFileSync(FILE_PATH, JSON.stringify(historico, null, 2), 'utf-8');
}

// Conecta ao RabbitMQ de forma ultra-resiliente a falhas de IP/Rede/DNS
async function connectRabbitMQ() {
    channel = null;
    connection = null;

    try {
        console.log('[RabbitMQ] Tentando conectar ao broker...');

        // Timeout de 4 segundos na tentativa de conexão para não travar o loop
        connection = await amqp.connect(RABBITMQ_URL, { timeout: 4000 });

        connection.on('error', (err) => {
            console.error('[RabbitMQ] Erro detectado na conexão global:', err.message);
        });

        connection.on('close', () => {
            console.error('[RabbitMQ] Conexão encerrada pelo Broker. Reiniciando busca em 5s...');
            setTimeout(connectRabbitMQ, 5000);
        });

        channel = await connection.createChannel();

        // Garante as filas e exchanges (Configuração com DLQ)
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await channel.assertQueue(DLX_QUEUE, { durable: true });
        await channel.bindQueue(DLX_QUEUE, DLX_EXCHANGE, 'erro_key');
        await channel.assertQueue(QUEUE_NAME, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': 'erro_key'
            }
        });

        console.log('[RabbitMQ] 🚀 Conectado com sucesso e filas prontas!');
        startWorker();

    } catch (error) {
        console.error('[RabbitMQ] ❌ Falha na tentativa de conexão:', error.message);
        channel = null;
        connection = null;
        console.log('[RabbitMQ] Agendando nova tentativa em 5 segundos...');
        setTimeout(connectRabbitMQ, 5000);
    }
}

// Rota para disparar novos relatórios
app.post('/solicitar-relatorio', async (req, res) => {
    const { tipoRelatorio, solicitadoPor } = req.body;
    const payload = {
        id: Math.floor(1000 + Math.random() * 9000),
        tipoRelatorio,
        solicitadoPor,
        timestamp: new Date().toLocaleTimeString()
    };

    const historico = lerHistorico();

    // Se o RabbitMQ estiver fora, retém no arquivo local com status de espera
    if (!channel) {
        historico.unshift({ ...payload, status: 'Aguardando o Broker voltar... ⏳' });
        salvarHistorico(historico);
        console.log(`[Produtor] Broker Offline! Relatório #${payload.id} retido no JSON.`);
        return res.status(202).json({ success: true, id: payload.id, info: "Salvo localmente. Aguardando conexão." });
    }

    // Se o RabbitMQ estiver online, segue o fluxo normal
    historico.unshift({ ...payload, status: 'Na Fila (Aguardando)' });
    salvarHistorico(historico);

    try {
        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), { persistent: true });
        console.log(`[Produtor] Relatório #${payload.id} enviado para a fila.`);
        return res.status(202).json({ success: true, id: payload.id });
    } catch (err) {
        console.error("[Produtor] Erro ao empurrar para a fila:", err.message);
        return res.status(500).json({ error: "Erro interno na fila." });
    }
});

// Rota para o frontend consultar o estado (Busca direto do JSON)
app.get('/status-relatorios', (req, res) => {
    res.json(lerHistorico());
});

// ================= WORKER (CONSUMIDOR) =================
function startWorker() {
    console.log('[Worker] Aguardando novas mensagens...');
    channel.prefetch(1);

    channel.consume(QUEUE_NAME, async (msg) => {
        if (msg === null) return;

        try {
            const conteudo = JSON.parse(msg.content.toString());

            // Simulação de Poison Message (Força erro na Auditoria de Estoque)
            if (conteudo.tipoRelatorio.includes("Estoque")) {
                console.log(`\n[Worker] ❌ Detectado erro crítico no formato do Relatório #${conteudo.id}!`);
                atualizarStatus(conteudo.id, 'Falhou - Enviado para análise (DLQ) ⚠️');
                channel.nack(msg, false, false);
                return;
            }

            // Fluxo feliz
            atualizarStatus(conteudo.id, 'Gerando Relatório... ⚙️');
            console.log(`[Worker] Processando Relatório #${conteudo.id}...`);

            await new Promise(resolve => setTimeout(resolve, 5000));

            atualizarStatus(conteudo.id, 'Concluído ✅');
            console.log(`[Worker] Relatório #${conteudo.id} finalizado!`);

            channel.ack(msg);
        } catch (err) {
            console.error('[Worker] Erro ao processar mensagem individual:', err.message);
            channel.nack(msg, false, false);
        }
    });
}

function atualizarStatus(id, novoStatus) {
    const historico = lerHistorico();
    const relatorio = historico.find(r => r.id === id);
    if (relatorio) {
        relatorio.status = novoStatus;
        salvarHistorico(historico);
    }
}

// Inicialização do Servidor
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
    connectRabbitMQ();
});

// ================= TRAVA DE SEGURANÇA GLOBAL =================
// Captura erros assíncronos e impede o "suicídio" do processo Node.js
process.on('uncaughtException', (err) => {
    console.error('💥 Erro global capturado (Evitou crash do app):', err.message);
});