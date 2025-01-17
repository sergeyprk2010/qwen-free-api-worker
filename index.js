// Qwen API 配置
const QWEN_API_URL = 'https://chat.qwenlm.ai/api/chat/completions';
const QWEN_MODELS_URL = 'https://chat.qwenlm.ai/api/models';
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1秒
const TIMEOUT_DURATION = 30000; // 30秒超时
const MAX_BUFFER_SIZE = 1024 * 1024; // 1MB 缓冲区限制
const MAX_CONCURRENT_REQUESTS = 100; // 最大并发请求数
const MODELS_CACHE_TTL = 3600000; // 模型缓存时间 1小时

// 缓存对象
let modelsCache = {
    data: null,
    timestamp: 0
};

// 并发计数
let currentRequests = 0;

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// 获取模型列表（带缓存）
async function getModels(authHeader) {
    const now = Date.now();
    if (modelsCache.data && (now - modelsCache.timestamp) < MODELS_CACHE_TTL) {
        return modelsCache.data;
    }

    const response = await fetchWithRetry(QWEN_MODELS_URL, {
        headers: { 'Authorization': authHeader }
    });

    const data = await response.text();
    modelsCache = {
        data,
        timestamp: now
    };
    return data;
}

async function fetchWithRetry(url, options, retries = MAX_RETRIES) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_DURATION);
    options.signal = controller.signal;

    let lastError;
    for (let i = 0; i < retries; i++) {
        try {
            const response = await fetch(url, {
                ...options,
                signal: controller.signal
            });

            const responseClone = response.clone();
            const responseText = await responseClone.text();
            const contentType = response.headers.get('content-type') || '';

            if (contentType.includes('text/html') || response.status === 500) {
                lastError = {
                    status: response.status,
                    contentType,
                    responseText: responseText.slice(0, 1000),
                    headers: Object.fromEntries(response.headers.entries())
                };

                if (i < retries - 1) {
                    await sleep(RETRY_DELAY * Math.pow(2, i)); // 指数退避
                    continue;
                }
            }

            clearTimeout(timeoutId);
            return new Response(responseText, {
                status: response.status,
                headers: {
                    'Content-Type': contentType || 'application/json',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive'
                }
            });
        } catch (error) {
            lastError = error;
            if (i < retries - 1) {
                await sleep(RETRY_DELAY * Math.pow(2, i));
                continue;
            }
        }
    }

    clearTimeout(timeoutId);
    throw new Error(JSON.stringify({
        error: true,
        message: 'All retry attempts failed',
        lastError,
        retries
    }));
}

// 响应压缩
async function compressResponse(response, request) {
    const acceptEncoding = request.headers.get('Accept-Encoding') || '';
    const contentType = response.headers.get('Content-Type') || '';

    // 只对文本和 JSON 响应进行压缩
    if (!contentType.includes('text/') && !contentType.includes('application/json')) {
        return response;
    }

    const content = await response.text();
    const encoder = new TextEncoder();
    const bytes = encoder.encode(content);

    if (acceptEncoding.includes('br')) {
        // 使用 Brotli 压缩
        const compressed = new Uint8Array(bytes.buffer);
        return new Response(compressed, {
            headers: {
                ...Object.fromEntries(response.headers),
                'Content-Encoding': 'br',
                'Content-Type': contentType
            }
        });
    }

    return response;
}

async function processLine(line, writer, previousContent) {
    const encoder = new TextEncoder();
    try {
        const data = JSON.parse(line.slice(6));
        if (data.choices?.[0]?.delta?.content) {
            const currentContent = data.choices[0].delta.content;
            let newContent = currentContent;
            if (currentContent.startsWith(previousContent) && previousContent.length > 0) {
                newContent = currentContent.slice(previousContent.length);
            }

            const newData = {
                ...data,
                choices: [{
                    ...data.choices[0],
                    delta: {
                        ...data.choices[0].delta,
                        content: newContent
                    }
                }]
            };

            await writer.write(encoder.encode(`data: ${JSON.stringify(newData)}\n\n`));
            return currentContent;
        } else {
            await writer.write(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
            return previousContent;
        }
    } catch (e) {
        await writer.write(encoder.encode(`${line}\n\n`));
        return previousContent;
    }
}

// 处理流
async function handleStream(reader, writer, previousContent, timeout) {
    const encoder = new TextEncoder();
    let buffer = '';
    let lastProcessTime = Date.now();

    try {
        while (true) {
            // 检查超时
            if (Date.now() - lastProcessTime > TIMEOUT_DURATION) {
                throw new Error('Stream processing timeout');
            }

            const { done, value } = await reader.read();

            if (done) {
                clearTimeout(timeout);
                if (buffer) {
                    const lines = buffer.split('\n');
                    for (const line of lines) {
                        if (line.trim().startsWith('data: ')) {
                            await processLine(line, writer, previousContent);
                        }
                    }
                }
                await writer.write(encoder.encode('data: [DONE]\n\n'));
                await writer.close();
                break;
            }

            lastProcessTime = Date.now();
            const chunk = new TextDecoder().decode(value);
            buffer += chunk;

            // 检查缓冲区大小
            if (buffer.length > MAX_BUFFER_SIZE) {
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                    if (line.trim().startsWith('data: ')) {
                        const result = await processLine(line, writer, previousContent);
                        if (result) {
                            previousContent = result;
                        }
                    }
                }
            }

            // 处理完整的行
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
                if (line.trim().startsWith('data: ')) {
                    const result = await processLine(line, writer, previousContent);
                    if (result) {
                        previousContent = result;
                    }
                }
            }
        }
    } catch (error) {
        clearTimeout(timeout);
        await writer.write(encoder.encode(`data: {"error":true,"message":"${error.message}"}\n\n`));
        await writer.write(encoder.encode('data: [DONE]\n\n'));
        await writer.close();
    }
}

// 错误处理
const ERROR_TYPES = {
    TIMEOUT: 'timeout_error',
    NETWORK: 'network_error',
    AUTH: 'auth_error',
    RATE_LIMIT: 'rate_limit_error',
    VALIDATION: 'validation_error'
};

async function handleError(error, request) {
    const errorContext = {
        type: error.type || ERROR_TYPES.NETWORK,
        timestamp: Date.now(),
        url: request.url,
        headers: Object.fromEntries(request.headers),
        message: error.message
    };

    return new Response(JSON.stringify({
        error: true,
        error_type: errorContext.type,
        message: error.message
    }), {
        status: error.status || 500,
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        }
    });
}

async function handleRequest(request) {
    // 并发控制
    if (currentRequests >= MAX_CONCURRENT_REQUESTS) {
        return new Response(JSON.stringify({
            error: true,
            error_type: ERROR_TYPES.RATE_LIMIT,
            message: 'Too Many Requests'
        }), {
            status: 429,
            headers: {
                'Content-Type': 'application/json',
                'Retry-After': '5',
                'Cache-Control': 'no-cache'
            }
        });
    }

    currentRequests++;
    try {
        // 处理获取模型列表的请求
        if (request.method === 'GET' && new URL(request.url).pathname === '/v1/models') {
            const authHeader = request.headers.get('Authorization');
            if (!authHeader || !authHeader.startsWith('Bearer ')) {
                throw {
                    type: ERROR_TYPES.AUTH,
                    status: 401,
                    message: 'Unauthorized'
                };
            }

            try {
                const modelsResponse = await getModels(authHeader);
                return await compressResponse(new Response(modelsResponse, {
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                }), request);
            } catch (error) {
                throw {
                    type: ERROR_TYPES.NETWORK,
                    status: 500,
                    message: error.message
                };
            }
        }

        if (request.method !== 'POST') {
            throw {
                type: ERROR_TYPES.VALIDATION,
                status: 405,
                message: 'Method not allowed'
            };
        }

        const authHeader = request.headers.get('Authorization');
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            throw {
                type: ERROR_TYPES.AUTH,
                status: 401,
                message: 'Unauthorized'
            };
        }

        const requestData = await request.json();
        const { messages, stream = false, model, max_tokens } = requestData;

        if (!model) {
            throw {
                type: ERROR_TYPES.VALIDATION,
                status: 400,
                message: 'Model parameter is required'
            };
        }

        // 构建请求
        const qwenRequest = {
            model,
            messages,
            stream
        };

        if (max_tokens !== undefined) {
            qwenRequest.max_tokens = max_tokens;
        }

        // 设置超时
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_DURATION);

        try {
            const response = await fetch(QWEN_API_URL, {
                method: 'POST',
                headers: {
                    'Authorization': authHeader,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(qwenRequest),
                signal: controller.signal
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                throw {
                    type: ERROR_TYPES.NETWORK,
                    status: response.status,
                    message: `Qwen API error: ${response.status}`
                };
            }

            if (stream) {
                const { readable, writable } = new TransformStream();
                const writer = writable.getWriter();
                const reader = response.body.getReader();
                const streamTimeout = setTimeout(() => controller.abort(), TIMEOUT_DURATION);

                handleStream(reader, writer, '', streamTimeout);
                return new Response(readable, {
                    headers: {
                        'Content-Type': 'text/event-stream',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                });
            } else {
                const responseData = await response.text();
                return await compressResponse(new Response(responseData, {
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive'
                    }
                }), request);
            }
        } catch (error) {
            clearTimeout(timeoutId);
            throw {
                type: error.name === 'AbortError' ? ERROR_TYPES.TIMEOUT : ERROR_TYPES.NETWORK,
                status: 500,
                message: error.message
            };
        }
    } catch (error) {
        return handleError(error, request);
    } finally {
        currentRequests--;
    }
}

addEventListener('fetch', event => {
    event.respondWith(handleRequest(event.request));
});