# WagerFi Background Worker 🚀

Background worker service for the WagerFi platform that handles on-chain wager resolutions, cancellations, and refunds.

## 🏗️ Architecture

- **Node.js + Express** server
- **Immediate execution** endpoints (no cron jobs, no edge functions)
- **Direct Supabase integration** for database operations
- **Solana integration** with WagerFi Token Program
- **CoinMarketCap API** for crypto price data
- **Simplified architecture** - everything in one place

## 🚀 Quick Start

### Local Development
```bash
npm install
cp .env.example .env
# Edit .env with your credentials
npm run dev
```

### Production Deployment
```bash
npm install
npm start
```

## 📡 API Endpoints

### Health & Status
- `GET /health` - Service health check
- `GET /status` - Current status and authority info

### Wager Operations
- `POST /create-wager` - Create new crypto or sports wagers
- `POST /accept-wager` - Accept existing wagers
- `POST /resolve-crypto-wager` - Resolve crypto wagers
- `POST /resolve-sports-wager` - Resolve sports wagers  
- `POST /cancel-wager` - Cancel wagers and refund creator
- `POST /handle-expired-wager` - Handle expired wagers

## 🔧 Environment Variables

**Required:**
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service role key
- `AUTHORITY_PRIVATE_KEY` - Authority private key for Solana transactions
- `WAGERFI_PROGRAM_ID` - WagerFi Token Program ID

**Optional:**
- `COINMARKETCAP_API_KEY` - For real crypto price data
- `PORT` - Server port (default: 3001)
- `NODE_ENV` - Environment (default: development)

## 🚀 Render.com Deployment

1. **Push to GitHub** with this repository
2. **Connect to Render.com** and select this repo
3. **Set environment variables** in Render dashboard
4. **Deploy** - Render will use the `render.yaml` file

## 🔒 Security Notes

- **Never commit** your `.env` file
- **Keep your authority private key** secure
- **Use environment variables** for all sensitive data
- **Monitor logs** for any suspicious activity

## 📊 Monitoring

- Health check endpoint for uptime monitoring
- Logs available in Render.com dashboard
- Error tracking and notification system

## 🧪 Testing

### Reward System Testing
Test the complete reward system with real SOL distributions:

```bash
# Run the comprehensive reward system test
node test-rewards.js

# Or test manually via API:
# 1. Check treasury balance
curl http://localhost:3001/admin/treasury-balance

# 2. Schedule test rewards (5 SOL budget)
curl -X POST http://localhost:3001/admin/test-rewards \
  -H "Content-Type: application/json" \
  -d '{"testBudget": 5.0}'

# 3. Distribute pending rewards
curl -X POST http://localhost:3001/admin/distribute-rewards
```

**What gets tested:**
- Treasury balance monitoring (6.5 SOL available)
- Random winner selection (10 winners × 0.5% = 5% of budget)
- Micro-drop distribution (100 recipients = 7% of budget)
- Real SOL transfers from treasury to users
- Database reward tracking and transaction logging
- User notifications for reward recipients

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

---

**Built with ❤️ by the WagerFi Team**
