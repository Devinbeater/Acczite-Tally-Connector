using System.Threading.Tasks;
using Acczite20.Models;

namespace Acczite20.Services
{
    public interface IRegistrationService
    {
        Task<(bool IsSuccess, string Message)> RegisterUserAsync(User user, string password);
    }
}
