#!/usr/bin/env python3
"""
Prosta pętla ReAct (Thought -> Action -> Observation) nad narzędziami MCP.
Narzędzia woła przekazany tool_executor, więc pętli jest obojętne, czy pod spodem jest stdio czy SSE.
"""

import asyncio
from dataclasses import dataclass, field
import json
import logging
import re
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("mcp_agent_loop")


@dataclass
class AgentStep:
    step_number: int
    thought: str
    action_tool: Optional[str] = None
    action_args: Optional[Dict[str, Any]] = None
    observation: Optional[Any] = None
    duration_ms: float = 0.0
    tokens_used: int = 0


@dataclass
class AgentExecutionResult:
    task: str
    final_answer: str
    success: bool
    total_steps: int
    steps: List[AgentStep] = field(default_factory=list)
    total_duration_ms: float = 0.0
    total_tokens: int = 0
    error_message: Optional[str] = None


@dataclass
class InferenceConfig:
    engine_name: str           # np. "ollama-cpu" vs "vllm-gpu" / "anthropic-api"
    model_id: str
    max_tokens: int            # 120 dla modelu dławionego, 1024 dla modelu mocnego
    temperature: float = 0.0   # 0, żeby przebiegi dało się powtórzyć
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    timeout: float = 60.0
    stop_sequences: Optional[List[str]] = None
    extra_options: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        # bez klucza API - to ląduje w plikach z wynikami
        return {
            "engine_name": self.engine_name,
            "model_id": self.model_id,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "base_url": self.base_url,
            "api_key": "***" if self.api_key else None,
            "timeout": self.timeout,
            "stop_sequences": self.stop_sequences,
            "extra_options": self.extra_options,
        }


def create_default_cpu_engine_config(
    model_id: str = "qwen2.5-coder:3b",
    max_tokens: int = 120,
    base_url: Optional[str] = None,
) -> InferenceConfig:
    """Silnik A: mały model na CPU, przycięty do 120 tokenów."""
    return InferenceConfig(
        engine_name="ollama-cpu",
        model_id=model_id,
        max_tokens=max_tokens,
        temperature=0.0,
        base_url=base_url or "http://localhost:11434",
    )


def create_default_gpu_engine_config(
    model_id: str = "qwen2.5-coder:7b",
    max_tokens: int = 1024,
    base_url: Optional[str] = None,
) -> InferenceConfig:
    """Silnik B: większy model, 1024 tokeny. Port 8000 w base_url = vLLM, inaczej Ollama."""
    engine_name = "vllm-gpu" if (base_url and "8000" in base_url) else "ollama-gpu"
    return InferenceConfig(
        engine_name=engine_name,
        model_id=model_id,
        max_tokens=max_tokens,
        temperature=0.0,
        base_url=base_url or "http://localhost:11434",
    )


def create_inference_engine(config: InferenceConfig) -> Callable[[str, Optional[List[Dict[str, str]]]], Any]:
    """
    Zwraca funkcję async (prompt, messages) -> (tekst, meta), dobraną po config.engine_name:
    "ollama*" -> /api/generate, "vllm*"/"openai*" -> /v1/chat/completions,
    "anthropic*" -> /v1/messages. Nieznana nazwa leci do Ollamy.
    """
    import httpx

    engine_type = config.engine_name.lower().strip()

    async def call_ollama(prompt: str, messages: Optional[List[Dict[str, str]]] = None) -> Tuple[str, Dict[str, Any]]:
        base = config.base_url or "http://localhost:11434"
        url = base if "/api/" in base else f"{base.rstrip('/')}/api/generate"
        # domyślne stopy pod generowanie pojedynczego zapytania SQL (koniec na średniku)
        stop = config.stop_sequences or [";", "```\n\n", "Task:", "Query:", "\n\n"]
        options = {
            "temperature": config.temperature,
            "num_predict": config.max_tokens,
            "stop": stop,
            **(config.extra_options or {}),
        }
        payload = {
            "model": config.model_id,
            "prompt": prompt,
            "stream": False,
            "options": options,
        }
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=config.timeout) as client:
            resp = await client.post(url, json=payload)
            lat_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code == 200:
                data = resp.json()
                text = data.get("response", "")
                tokens = data.get("eval_count", max(1, len(text) // 4))
                return text, {"tokens_used": tokens, "duration_ms": lat_ms, "raw": data}
            else:
                raise RuntimeError(f"Ollama error {resp.status_code}: {resp.text}")

    async def call_openai_compatible(prompt: str, messages: Optional[List[Dict[str, str]]] = None) -> Tuple[str, Dict[str, Any]]:
        base = config.base_url or "http://localhost:8000/v1"
        url = f"{base.rstrip('/')}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if config.api_key:
            headers["Authorization"] = f"Bearer {config.api_key}"
        msg_list = messages or [{"role": "user", "content": prompt}]
        payload = {
            "model": config.model_id,
            "messages": msg_list,
            "max_tokens": config.max_tokens,
            "temperature": config.temperature,
            **(config.extra_options or {}),
        }
        if config.stop_sequences:
            payload["stop"] = config.stop_sequences
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=config.timeout) as client:
            resp = await client.post(url, headers=headers, json=payload)
            lat_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code == 200:
                data = resp.json()
                choices = data.get("choices", [])
                text = choices[0]["message"]["content"] if choices else ""
                usage = data.get("usage", {})
                tokens = usage.get("completion_tokens", max(1, len(text) // 4))
                return text, {"tokens_used": tokens, "duration_ms": lat_ms, "raw": data}
            else:
                raise RuntimeError(f"OpenAI/vLLM error {resp.status_code}: {resp.text}")

    async def call_anthropic(prompt: str, messages: Optional[List[Dict[str, str]]] = None) -> Tuple[str, Dict[str, Any]]:
        base = config.base_url or "https://api.anthropic.com/v1"
        url = f"{base.rstrip('/')}/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": config.api_key or "",
            "anthropic-version": "2023-06-01",
        }
        msg_list = messages or [{"role": "user", "content": prompt}]
        payload = {
            "model": config.model_id,
            "max_tokens": config.max_tokens,
            "temperature": config.temperature,
            "messages": msg_list,
            **(config.extra_options or {}),
        }
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=config.timeout) as client:
            resp = await client.post(url, headers=headers, json=payload)
            lat_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("content", [])
                text = content[0].get("text", "") if content else ""
                usage = data.get("usage", {})
                tokens = usage.get("output_tokens", max(1, len(text) // 4))
                return text, {"tokens_used": tokens, "duration_ms": lat_ms, "raw": data}
            else:
                raise RuntimeError(f"Anthropic error {resp.status_code}: {resp.text}")

    if "ollama" in engine_type:
        return call_ollama
    elif "vllm" in engine_type or "openai" in engine_type:
        return call_openai_compatible
    elif "anthropic" in engine_type:
        return call_anthropic
    else:
        return call_ollama


class ReActAgentLoop:
    def __init__(
        self,
        tool_executor: Callable[[str, Dict[str, Any]], Any],
        llm_caller: Optional[Callable[[str, List[Dict[str, str]]], Any]] = None,
        max_iterations: int = 10,
        system_prompt: Optional[str] = None,
        inference_config: Optional[InferenceConfig] = None,
    ) -> None:
        """
        Trzeba podać llm_caller albo inference_config (wtedy ma pierwszeństwo).
        W testach llm_caller to zwykle atrapa zwracająca gotowy tekst.
        """
        self.tool_executor = tool_executor
        if inference_config is not None:
            self.inference_config = inference_config
            self.llm_caller = create_inference_engine(inference_config)
        elif llm_caller is not None:
            self.inference_config = None
            self.llm_caller = llm_caller
        else:
            raise ValueError("Należy przekazać co najmniej 'llm_caller' lub 'inference_config'.")
        self.max_iterations = max_iterations
        self.system_prompt = system_prompt or (
            "Jesteś wyspecjalizowanym asystentem analitycznym baz danych. "
            "Używaj schematu ReAct:\n"
            "Thought: [analiza problemu]\n"
            "Action: nazwa_narzędzia(argumenty_w_formacie_json)\n"
            "Observation zostanie dostarczona przez serwer MCP.\n"
            "Gdy uzyskasz wynik końcowy, napisz:\n"
            "Thought: Znam odpowiedź.\n"
            "Final Answer: [odpowiedź końcowa]"
        )

    def _parse_action(self, text: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Szuka `Action: tool({...})`, a jak nie ma, to bloku ```json {"action": ..., "args": {...}}```.
        Regex jest leniwy, więc argumenty z nawiasem ")" w środku się utną - na razie nie przeszkadzało.
        """
        match = re.search(r"Action:\s*([a-zA-Z0-9_]+)\s*\((.*?)\)", text, re.DOTALL)
        if match:
            tool_name = match.group(1).strip()
            args_raw = match.group(2).strip()
            try:
                args = json.loads(args_raw) if args_raw else {}
                return tool_name, args
            except json.JSONDecodeError:
                # małe modele często piszą {'a': 1} zamiast JSON-a
                try:
                    fixed = args_raw.replace("'", '"')
                    return tool_name, json.loads(fixed)
                except Exception:
                    logger.warning("Błąd parsowania argumentów narzędzia: %s", args_raw)
                    return tool_name, {"raw_argument": args_raw}

        json_match = re.search(r"```json\s*\{\s*\"action\":\s*\"([^\"]+)\",\s*\"args\":\s*(\{.*?\})\s*\}\s*```", text, re.DOTALL)
        if json_match:
            try:
                return json_match.group(1), json.loads(json_match.group(2))
            except Exception:
                pass

        return None, None

    async def run(self, task: str) -> AgentExecutionResult:
        t_start = time.perf_counter()
        steps: List[AgentStep] = []
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"Zadanie: {task}"},
        ]

        final_answer = ""
        success = True
        error_message = None

        for step_idx in range(1, self.max_iterations + 1):
            t_step_start = time.perf_counter()
            prompt = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in messages])

            try:
                response = await self.llm_caller(prompt, messages)
            except Exception as exc:
                success = False
                error_message = f"Błąd inferencji LLM w kroku {step_idx}: {exc}"
                break

            step_tokens = 0
            if isinstance(response, tuple):
                response_text = response[0]
                if len(response) > 1 and isinstance(response[1], dict):
                    step_tokens = response[1].get("tokens_used", 0)
            elif isinstance(response, str):
                response_text = response
            else:
                response_text = str(response)

            if "Final Answer:" in response_text:
                final_answer = response_text.split("Final Answer:")[-1].strip()
                thought_part = response_text.split("Final Answer:")[0].replace("Thought:", "").strip()
                t_step_end = time.perf_counter()
                steps.append(
                    AgentStep(
                        step_number=step_idx,
                        thought=thought_part,
                        duration_ms=(t_step_end - t_step_start) * 1000.0,
                        tokens_used=step_tokens,
                    )
                )
                break

            thought_part = response_text
            if "Action:" in response_text:
                thought_part = response_text.split("Action:")[0].replace("Thought:", "").strip()

            tool_name, tool_args = self._parse_action(response_text)

            observation = None
            if tool_name:
                try:
                    observation = await self.tool_executor(tool_name, tool_args or {})
                except Exception as exc:
                    observation = f"BŁĄD WYWOŁANIA NARZĘDZIA: {exc}"

                obs_str = observation if isinstance(observation, str) else json.dumps(observation, ensure_ascii=False)
                messages.append({"role": "assistant", "content": response_text})
                messages.append({"role": "user", "content": f"Observation: {obs_str}"})
            else:
                messages.append({"role": "assistant", "content": response_text})
                messages.append({"role": "user", "content": "Wymagane jest podanie akcji (Action: tool_name(...)) lub odpowiedzi końcowej (Final Answer: ...)."})

            t_step_end = time.perf_counter()
            steps.append(
                AgentStep(
                    step_number=step_idx,
                    thought=thought_part,
                    action_tool=tool_name,
                    action_args=tool_args,
                    observation=observation,
                    duration_ms=(t_step_end - t_step_start) * 1000.0,
                    tokens_used=step_tokens,
                )
            )

        t_total_end = time.perf_counter()
        total_duration_ms = (t_total_end - t_start) * 1000.0
        total_tokens = sum(s.tokens_used for s in steps)

        return AgentExecutionResult(
            task=task,
            final_answer=final_answer,
            success=success,
            total_steps=len(steps),
            steps=steps,
            total_duration_ms=total_duration_ms,
            total_tokens=total_tokens,
            error_message=error_message,
        )
