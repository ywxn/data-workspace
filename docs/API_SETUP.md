# API Configuration Guide

## Supported LLM Providers

This application supports two leading AI providers:

1. **OpenAI** (Default)
   - Model: GPT-4 Turbo or GPT-4o
   - Best for: Code generation, complex analysis
   - Pricing: Token-based (input + output tokens)

2. **Anthropic Claude**
   - Model: Claude 3.5 Sonnet
   - Best for: Long-context analysis, creative tasks
   - Pricing: Token-based with bulk discounts

## Getting API Keys

### OpenAI

1. Visit https://platform.openai.com/account/api-keys
2. Sign up or log in with your account
3. Click "Create new secret key"
4. Copy the key (starts with `sk-`)
5. Store securely in the application

**Free Tier**: $5 credit (expires after 3 months)
**Paid**: Pay-as-you-go, no monthly minimum

### Anthropic (Claude)

1. Visit https://console.anthropic.com/account/keys
2. Sign up or log in with your account
3. Click "Create a new key"
4. Copy the key (starts with `sk-ant-`)
5. Store securely in the application

**Free Tier**: $5 credit
**Paid**: Pay-as-you-go pricing

## Configuration Methods

### Method 1: GUI Setup (Recommended for Users)

1. **First Launch**:
   ```bash
   python gui_frontend.py
   ```

2. **API Key Configuration Dialog**:
   - Select AI provider (OpenAI or Claude)
   - Enter API key from clipboard
   - Optionally configure both providers
   - Keys saved to `config.json`

3. **In-App Settings** (if implemented):
   - Access settings menu
   - Update/change API keys anytime
   - Switch between providers

### Method 2: config.json (For Developers)

Edit `config.json` directly:

```json
{
  "api_keys": {
    "openai": "sk-YOUR_OPENAI_KEY_HERE",
    "claude": "sk-ant-YOUR_CLAUDE_KEY_HERE"
  },
  "default_api": "openai"
}
```

**Security Note**: Never commit this file to version control.

### Method 3: Environment Variables

```bash
# Windows (Command Prompt)
set OPENAI_API_KEY=sk-YOUR_KEY_HERE
set ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE

# Windows (PowerShell)
$env:OPENAI_API_KEY = "sk-YOUR_KEY_HERE"
$env:ANTHROPIC_API_KEY = "sk-ant-YOUR_KEY_HERE"

# macOS/Linux (Bash/Zsh)
export OPENAI_API_KEY=sk-YOUR_KEY_HERE
export ANTHROPIC_API_KEY=sk-ant-YOUR_KEY_HERE

# Permanent (add to ~/.bashrc or ~/.zshrc)
echo 'export OPENAI_API_KEY="sk-YOUR_KEY_HERE"' >> ~/.bashrc
source ~/.bashrc
```

## Switching Providers

### Change Default Provider

Edit `config.json`:
```json
{
  "api_keys": {
    "openai": "sk-...",
    "claude": "sk-ant-..."
  },
  "default_api": "claude"  // Switch to Claude
}
```

### At Runtime (In Code)

```python
from agents import AIAgent

# Use specific provider
agent = AIAgent(api_provider="claude")

# Or
agent = AIAgent(api_provider="openai")
```

### Via GUI (If Available)

- Settings menu → Default Provider
- Or select in project/chat initialization

## Updating Models

Models are configured in `constants.py`:

```python
LLM_MODELS = {
    "claude": "claude-3-5-sonnet-20241022",
    "openai": "gpt-4o-2024-08-06"
}
```

**Check Latest Models**:
- OpenAI: https://platform.openai.com/docs/models/gpt-4-turbo
- Anthropic: https://docs.anthropic.com/claude/reference/models/latest

**Update Process**:
1. Check availability on provider's website
2. Update desired model name in `constants.py`
3. Restart application

## Pricing and Cost Optimization

### OpenAI Pricing (GPT-4o)

- **Input**: $0.005 / 1K tokens
- **Output**: $0.015 / 1K tokens
- **Typical query**: $0.01 - $0.05

### Claude Pricing

- **Input**: $0.003 / 1K tokens
- **Output**: $0.015 / 1K tokens
- **Typical query**: $0.01 - $0.05

### Cost Optimization Tips

1. **Reduce Token Usage**:
   ```python
   # In constants.py, reduce max tokens
   LLM_MAX_TOKENS_CODE = 500  # From 1000
   LLM_MAX_TOKENS_ANALYSIS = 500  # From 1000
   ```

2. **Use Exact Queries**:
   - More specific → shorter code → lower cost
   - "Show top 5 products by sales" (better)
   - Vs. "Analyze my product data" (requires more processing)

3. **Batch Operations**:
   - Ask multiple questions before changing data source
   - Reduces API initialization overhead

4. **Cache Results**:
   - Save analysis results
   - Reference previous results for similar queries

## Troubleshooting API Issues

### "API Key Not Found"

**Check**:
1. `config.json` exists and has valid key
2. Environment variables are set (if using that method)
3. API key format is correct:
   - OpenAI: `sk-` prefix
   - Claude: `sk-ant-` prefix

**Fix**:
```bash
# Verify key format
python
>>> from config import ConfigManager
>>> key = ConfigManager.get_api_key("openai")
>>> print(f"Key present: {key is not None}")
>>> print(f"Key starts with sk-: {key.startswith('sk-') if key else False}")
```

### "Unauthorized" or "Invalid API Key"

**Possible Causes**:
1. Key is valid in provider console but hasn't been copied correctly
2. Key was revoked or deleted
3. Account doesn't have API access enabled
4. Typo in key

**Solution**:
1. Log into provider console
2. Verify key status (should be "Active")
3. Delete old key and create new one
4. Copy carefully, paste into app
5. Restart application

### "Rate Limited"

**Meaning**: Too many requests in short time

**Solutions**:
- Wait a few seconds between queries
- Increase delay in code (if applicable)
- Upgrade account for higher limits

**Check Rate Limits**:
- OpenAI: https://platform.openai.com/account/rate-limits
- Anthropic: Check quota in account settings

### "Context Length Exceeded"

**Meaning**: DataFrame + prompt too large for model

**Solutions**:
1. Filter data before analysis
   ```python
   # In query: "Show top 100 rows of sales data from last month"
   ```
2. Use smaller dataset
3. Increase context limit (if available in paid tier)

### "No Credentials Found"

The application can't find any API keys.

**Check in order**:
1. `config.json` exists with valid key
2. Environment variables are set
3. Restart application after setting variables

```bash
# Test environment variable
python -c "import os; print(os.getenv('OPENAI_API_KEY'))"
```

## Security Best Practices

### Protecting API Keys

1. **Never commit keys to git**:
   ```bash
   # Add to .gitignore
   echo "config.json" >> .gitignore
   ```

2. **Use environment variables in production**:
   ```bash
   export OPENAI_API_KEY="your-key-here"
   export ANTHROPIC_API_KEY="your-key-here"
   ```

3. **Restrict key permissions**:
   - Delete unused keys
   - Rotate keys periodically
   - Use key scoping if available

4. **Monitor usage**:
   - OpenAI: https://platform.openai.com/account/usage
   - Anthropic: Check console for usage

### Rotating Keys

**If key is compromised**:
1. Go to provider's console
2. Delete the compromised key
3. Create a new key
4. Update in `config.json` or environment
5. Restart application

## API Quota and Limits

### OpenAI

Default limits (Free tier):
- Requests: 3/min
- Tokens: 90K/min

Increase by adding payment method.

### Anthropic

Depends on plan:
- Free: Limited rate
- Paid: Higher based on account tier

## Multiple API Keys (Optional)

Store multiple keys for different environments:

```json
{
  "api_keys": {
    "openai": {
      "development": "sk-...",
      "production": "sk-..."
    },
    "claude": "sk-ant-..."
  },
  "default_api": "openai"
}
```

Then select which to use at runtime.

## Fallback Strategy

If primary provider fails:

```python
from agents import AIAgent
try:
    agent = AIAgent(api_provider="openai")
except Exception:
    # Fallback to Claude
    agent = AIAgent(api_provider="claude")
```

## Testing API Connection

```python
# Test script
from config import ConfigManager
from agents import AIAgent
import pandas as pd

# Check configuration
print("OpenAI Key:", ConfigManager.get_api_key("openai") is not None)
print("Claude Key:", ConfigManager.get_api_key("claude") is not None)

# Test API
try:
    agent = AIAgent(api_provider="openai")
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    
    import asyncio
    response = asyncio.run(agent.execute_query("Show me the data", df))
    print("API test successful!")
    print(response)
except Exception as e:
    print(f"API test failed: {e}")
```

Run it:
```bash
python test_api.py
```

## Advanced Configuration

### Custom Model Parameters

Modify `constants.py`:

```python
# Token limits
LLM_MAX_TOKENS_DEFAULT = 800
LLM_MAX_TOKENS_CODE = 2000      # Increase for complex code
LLM_MAX_TOKENS_ANALYSIS = 1500  # Increase for detailed analysis

# Temperature (randomness)
LLM_TEMPERATURE_DEFAULT = 0.3       # Lower = more deterministic
LLM_TEMPERATURE_CODE = 0.2          # Very deterministic for code
LLM_TEMPERATURE_ANALYSIS = 0.7      # More creative for analysis
```

### Custom Models

Change model versions:

```python
LLM_MODELS = {
    "claude": "claude-3-opus-20240229",  # Older Claude 3
    "openai": "gpt-4-turbo-preview"     # Alternative GPT-4
}
```

## Support and Limits

For help:
1. Check API provider's documentation
2. Review application logs: `logs/app.log`
3. Test with smaller datasets first
4. Contact provider support if key issues persist
