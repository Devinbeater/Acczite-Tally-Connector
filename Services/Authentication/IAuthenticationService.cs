using System.Threading.Tasks;

namespace Acczite20.Services.Authentication
{
    public interface IAuthenticationService
    {
        Task<bool> LoginAsync(string identifier, string password);
    }
}
