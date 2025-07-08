## Standard Workflow
1. **Plan first**  
   • Think through the problem.  
   • Write a *Plan* section to `tasks/todo.md` using markdown check-boxes (`- [ ] task`).  
2. **Pause for approval** – wait until Evan gives the OK.  
3. **Execute** one task at a time, replacing `[ ]` with `[x]` as each finishes.  
4. **After every task**: reply with a high-level summary of the change.  
5. **Keep it simple** – smallest viable change, minimal blast radius.  
6. **When all tasks are complete**: append a *Review* section to `tasks/todo.md` summarising what changed and anything notable.

## Database Development Pipeline
**Local Development Environment:**
- **Local PostgreSQL**: `localhost:54322` (Supabase extension)
- **Local Studio**: `localhost:54323` (visual interface)
- **Local Project**: `EVO2` (isolated development)

**Production Environment:**
- **Remote Project**: CAN Alternative Data (`owlxpknauzejswlagwdi.supabase.co`)
- **ETL Script**: `lobbying_enhanced.py` (local PostgreSQL only)

**Development Workflow:**
1. **Build locally** using `localhost:54322` - fast iteration, safe testing
2. **Create migrations** for database schema changes
3. **Test ETL scripts** with local data
4. **Push to remote** when ready: `supabase db push --linked`
5. **Deploy to production** CAN Alternative Data project

**Current Setup:**
- Local PostgreSQL connection for development
- High-performance PostgreSQL COPY operations
- Streamlined local-only ETL script  
