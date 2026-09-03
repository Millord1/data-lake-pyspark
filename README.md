http://localhost:8085

```bash
sudo docker compose -f docker/docker-compose.test.yml logs airflow | grep -i -E "username|password"
```